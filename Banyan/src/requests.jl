#################
# Magic Methods #
#################

# TODO: Implement magic methods

# Assume that this is mutating
# function Base.getproperty(fut::Future, sym::Symbol)
# end

# Mutating
# TODO: put this back in some way
# function Base.setproperty!(fut::Future, sym::Symbol, new_value)
# end

#############################
# Basic methods for futures #
#############################

 function check_worker_stuck_error(
    value_id::ValueId,
    contents::String,
    error_for_main_stuck::Union{Nothing,String},
    error_for_main_stuck_time::Union{Nothing,DateTime}
)::Tuple{Union{Nothing,String},Union{Nothing,DateTime}}
    if value_id == "-2" && isnothing(error_for_main_stuck_time)
        error_for_main_stuck_msg::String = from_jl_string(contents)
        if contains(error_for_main_stuck_msg, "session $(get_session_id())")
            error_for_main_stuck = error_for_main_stuck_msg
            error_for_main_stuck_time = Dates.now()
        end
    end
    error_for_main_stuck, error_for_main_stuck_time
end

function check_worker_stuck(
    error_for_main_stuck::Union{Nothing,String},
    error_for_main_stuck_time::Union{Nothing,DateTime}
)::Union{Nothing,String}
    if !isnothing(error_for_main_stuck) && !isnothing(error_for_main_stuck_time) && (Dates.now() - error_for_main_stuck_time) > Second(10)
        println(error_for_main_stuck)
        @warn "The above error occurred on some workers but other workers are still running. Please interrupt and end the session unless you expect that a lot of logs are being returned."
        error_for_main_stuck = nothing
    end
    error_for_main_stuck
end

destroyed_value_ids = ValueId[]

function get_destroyed_value_ids()
    global destroyed_value_ids
    destroyed_value_ids
end

function _partitioned_computation_concrete(fut::Future, destination::Location, new_source::Location, sessions::Dict{SessionId,Session}, session_id::SessionId, session::Session, resource_id::ResourceId, destroyed_value_ids::Base.Vector{ValueId})
    @partitioned fut begin end

    # Get all tasks to be recorded in this call to `compute`
    tasks::Vector{DelayedTask} = DelayedTask[req.task for req in session.pending_requests if req isa RecordTaskRequest]
    tasks_reverse::Vector{DelayedTask} = reverse(tasks)

    # Call `partitioned_using_func`s in 2 passes - forwards and backwards.
    # This allows sample properties to propagate in both directions. We
    # must also make sure to apply mutations in each task appropriately.
    for t in tasks_reverse
        apply_mutation(t.mutation, true)
    end
    for t in tasks
        set_task(t)
        if !isnothing(t.partitioned_using_func)
            apply_partitioned_using_func(t.partitioned_using_func)
        end
        apply_mutation(t.mutation, false)
    end
    for t in tasks_reverse
        set_task(t)
        apply_mutation(t.mutation, true)
        if !isnothing(t.partitioned_using_func)
            apply_partitioned_using_func(t.partitioned_using_func)
        end
    end

    # Do further processing on tasks now that all samples have been
    # computed and sample properties have been set up to share references
    # as needed to prevent expensive redundant computation of sample
    # properties like divisions
    for t::DelayedTask in tasks
        apply_mutation(t.mutation, false)
        
        # Call `partitioned_with_func` to create additional PAs for each task
        set_task(t)
        if t.partitioned_with_func != identity
            partitioned_with_func::Function = t.partitioned_with_func
            partitioned_with_func(t.futures)
        end

        # Cascade PAs backwards. In other words, if as we go from first to
        # last PA we come across one that's annotating a value not
        # annotated in a previous PA, we copy over the annotation (the
        # assigned PT stack) to the previous PA.
        for (j, pa::PartitionAnnotation) in enumerate(t.pa_union)
            # For each PA in this PA union for this task, we consider the
            # PAs before it
            for previous_pa::PartitionAnnotation in reverse(t.pa_union[1:j-1])
                for value_id::ValueId in keys(pa.partitions.pt_stacks)
                    # Check if there is a previous PA where this value
                    # does not have a PT.
                    if !haskey(previous_pa.partitions.pt_stacks, value_id)
                        # Cascade the PT composition backwards
                        previous_pa.partitions.pt_stacks[value_id] =
                            deepcopy(pa.partitions.pt_stacks[value_id])

                        # Cascade backwards all constraints that mention the
                        # value. NOTE: If this is not desired, users should
                        # be explicit and assign different PT compositions for
                        # different values.
                        for constraint::PartitioningConstraint in pa.constraints.constraints
                            # Determine whether we should copy over this constraint
                            copy_constraint::Bool = false
                            if !isempty(constraint.args)
                                for arg in constraint.args
                                    arg_v::ValueId = arg[1]
                                    if arg_v == value_id
                                        copy_constraint = true
                                    end
                                end
                            elseif !isempty(constraint.co_args)
                                for arg::Vector{PartitionTypeReference} in constraint.co_args
                                    for subarg::PartitionTypeReference in arg
                                        subarg_v = subarg[1]
                                        if subarg_v == value_id
                                            copy_constraint = true
                                        end    
                                    end
                                end
                            end

                            # Copy over constraint
                            if copy_constraint
                                push!(previous_pa.constraints.constraints, deepcopy(constraint))
                            end
                        end
                    end
                end
            end
        end
    end

    # Switch back to a new task for next code region
    finish_task()

    # Iterate through tasks for further processing before recording them
    forget_parents()
    for t::DelayedTask in tasks
        # Apply defaults to PAs
        for pa::PartitionAnnotation in t.pa_union
            apply_default_constraints!(pa)
            duplicate_for_batching!(pa)
        end

        # Destroy all closures so that all references to `Future`s are dropped
        t.partitioned_using_func = NOTHING_PARTITIONED_USING_FUNC
        t.partitioned_with_func = identity
        empty!(t.futures)

        # Handle 
        empty!(t.mutation) # Drop references to `Future`s here as well

        t.input_value_ids = map(value_id_getter, t.inputs)
        t.output_value_ids = map(value_id_getter, t.outputs)
        t.scaled_value_ids = map(value_id_getter, t.scaled)
        empty!(t.inputs)
        empty!(t.outputs)
        empty!(t.scaled)

        # @show statements for displaying info about each task
        if Banyan.INVESTIGATING_TASKS
            @show t.memory_usage
            @show t.inputs
            @show t.outputs
            @show t.code
            @show t.value_names
            @show t.effects
            @show t.pa_union
        end
    end

    # Finalize (destroy) all `Future`s that can be destroyed
    GC.gc()

    # Destroy everything that is to be destroyed in this task
    for req in session.pending_requests
        # Don't destroy stuff where a `DestroyRequest` was produced just
        # because of a `mutated(old, new)`
        if req isa DestroyRequest
            req_value_id::ValueId = req.value_id
            push!(destroyed_value_ids, req_value_id)
            if Banyan.INVESTIGATING_DESTROYING_FUTURES
                @show req_value_id
            end
            # If this value was to be downloaded to or uploaded from the
            # client side, delete the reference to its data. We do the
            # `GC.gc()` before this and store `futures_on_client` in a
            # `WeakKeyDict` just so that we can ensure that we can actually
            # garbage-collect a value if it's done and only keep it around if
            # a later call to `collect` it may happen and in that call to
            # `collect` we will be using `partitioned_computation` to
            # communicate with the executor and fill in the value for that
            # future as needed and then return `the_future.value`.
            if haskey(session.futures_on_client, req_value_id)
                delete!(session.futures_on_client, req_value_id)
            end

            # Remove information about the value's location including the
            # sample taken from it
            delete!(session.locations, req_value_id)
            # delete_same_stastics(req_value_id)
        end
    end
    # end of preparing tasks

    # Send evaluation request
    is_merged_to_disk::Bool = false
    try
        response = send_evaluation(fut.value_id, session_id)
        is_merged_to_disk = response["is_merged_to_disk"]::Bool
    catch
        end_session(failed=true)
        rethrow()
    end

    # Get queues for moving data between client and cluster
    scatter_queue = scatter_queue_url()
    gather_queue = gather_queue_url()

    # There are two cases: either we
    # TODO: Maybe we don't need to wait_For_session

    # There is a problem where we start a session with nowait=true and then it
    # reuses a resource that is in a creating state. Since the session is still
    # creating and we have not yet waited for it to start, if we have
    # `estimate_available_memory=false` then we will end up with job info not
    # knowing the available memory and unable to schedule. We definitely should
    # ensure that no resource is ending up in a creating state when it has
    # been clearly destroyed and that is a problem to fix with destroy-sessions.
    # However, there are some options to address this:
    # - wait_for_session before calling get_session_status
    # - always estimate_available_memory if we do nowait
    # - always only wait for session at the start since if you're loading data
    # then you will probably do sample collection first and need to wait for
    # session anyway
    # We'll go with the last one and understand that you will pay the price of
    # loading packages twice. This is bad but even if you created the session
    # without waiting, you would still have to suffer from intiial compilation
    # time for functions on the client side and on the executor and that can't
    # be overlapped. The best we can do is try to create sysimages for Banyan*
    # libraries to reduce the package loading time on the executor.

    # Read instructions from gather queue
    @debug "Waiting on running session $session_id, listening on $gather_queue, and computing value with ID $(fut.value_id)"
    p = ProgressUnknown("Computing value with ID $(fut.value_id)", spinner=true)
    error_for_main_stuck::Union{Nothing,String} = nothing
    error_for_main_stuck_time::Union{Nothing,DateTime} = nothing
    while true
        # TODO: Use to_jl_value and from_jl_value to support Client
        message, error_for_main_stuck = sqs_receive_next_message(gather_queue, p, error_for_main_stuck, error_for_main_stuck_time)
        message_type::String = message["kind"]
        if message_type == "SCATTER_REQUEST"
            # Send scatter
            value_id = message["value_id"]::ValueId
            haskey(session.futures_on_client, value_id) || error("Expected future to be stored on client side")
            f = session.futures_on_client[value_id]::Future
            # @debug "Received scatter request for value with ID $value_id and value $(f.value) with location $(get_location(f))"
            sqs_send_message(
                scatter_queue,
                JSON.json(
                    Dict{String,Any}(
                        "value_id" => value_id,
                        "contents" => to_jl_string(f.value)
                    ),
                ),
            )
            # TODO: Update stale/mutated here to avoid costly
            # call to `send_evaluation`
        elseif message_type == "GATHER"
            # Receive gather
            value_id = message["value_id"]::ValueId
            num_chunks = message["num_chunks"]::Int64
            num_remaining_chunks = num_chunks - 1
            
            whole_message_contents = if num_chunks > 1
                partial_messages = Vector{String}(undef, num_chunks)
                partial_messages[message["chunk_idx"]] = message["contents"]
                @sync for i = 1:num_remaining_chunks
                    @async begin
                        partial_message, _ = sqs_receive_next_message(gather_queue, p, nothing, nothing)
                        chunk_idx = partial_message["chunk_idx"]
                        partial_messages[chunk_idx] = message["contents"]
                    end
                end
                join(partial_messages)
            else
                message["contents"]
            end

            if haskey(session.futures_on_client, value_id)
                value = from_jl_string(whole_message_contents)
                f = session.futures_on_client[value_id]::Future
                f.value = value
                # TODO: Update stale/mutated here to avoid costly
                # call to `send_evaluation`
            end

            error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(value_id, contents, error_for_main_stuck, error_for_main_stuck_time)
        elseif message_type == "EVALUATION_END"
            if message["end"]::Bool == true
                break
            end
        end
    end

    # Update `mutated` and `stale` for the future that is being evaluated
    fut.mutated = false
    # TODO: See if there are more cases where you a `compute` call on a future
    # makes it no longer stale
    if get_dst_name(fut) == "Client"
        fut.stale = false
    end

    # This is where we update the location source.
    use_new_source_func::Bool = false
    if is_merged_to_disk
        sourced(fut, Disk())
    else
        if !isnothing(new_source)
            sourced(fut, new_source)
        else
            use_new_source_func = true
        end
    end

    # Reset the location destination to its default. This is where we
    # update the location destination.
    destined(fut, None())

    use_new_source_func
end

function partitioned_computation_concrete(
    @nospecialize(handler::Function),
    fut::Future,
    destination::Location,
    @nospecialize(new_source::Location),
    @nospecialize(new_source_func::Function)
)

    # NOTE: Right now, `compute` wil generally spill to disk (or write to some
    # remote location or download to the client). It will not just persist data
    # in memory. In Spark or Dask, it is recommended that you call persist or
    # compute or something like that in order to cache data in memory and then
    # ensure it stays there as you do logistic regression or some iterative
    # computation like that. With an iterative computation like logistic
    # regression in Banyan, you would only call `compute` on the result and we
    # would be using a same future for each iteration. Each iteration would
    # correspond to a separate stage with casting happening between them. And
    # the scheduler would try as hard as possible to keep the whole thing in
    # memory. This is because unlike Dask, we allow a Future to be reused
    # across tasks. If we need `compute` to only evaluate and persist in-memory
    # we should modify the way we schedule the final merging stage to not
    # require the last value to be merged simply because it is being evaluated.

    sessions = get_sessions_dict()
    session = get_session()
    session_id = get_session_id()
    resource_id = session.resource_id


    destroyed_value_ids = get_destroyed_value_ids()
    if fut.value_id in destroyed_value_ids
        throw(ArgumentError("Cannot compute a destroyed future"))
    end

    destination_dst_name::String = destination.dst_name
    if fut.mutated || (destination_dst_name == "Client" && fut.stale) || destination_dst_name == "Remote"
        # TODO: Check to ensure that `fut` is annotated
        # This creates an empty final task that ensures that the future
        # will be scheduled to get sent to its destination.
        destined(fut, destination)
        mutated(fut)
        partitioned_with(handler, [fut], scaled=[fut])
        
        use_new_source_func = _partitioned_computation_concrete(fut, destination, new_source, sessions, session_id, session, resource_id, destroyed_value_ids)
        # TODO: If not still merged to disk, we need to lazily set the location source to something else
        if use_new_source_func
            if new_source_func !== identity
                sourced(fut, new_source_func)
            else
                # TODO: Maybe suppress this warning because while it may be
                # useful for large datasets, it is going to come up for
                # every aggregateion result value that doesn't have a source
                # but is being computed with the Client as its location.
                if destination.src_name == "None"
                    # It is not guaranteed that this data can be used again.
                    # In fact, this data - or rather, this value - can only be
                    # used again if it is in memory. But because it is up to
                    # the schedule to determine whether it is possible for the
                    # data to fit in memory, we can't be sure that it will be
                    # in memory. So this data should have first been written to
                    # disk with `compute_inplace` and then only written to this
                    # unreadable location.
                    @warn "Value with ID $(fut.value_id) has been written to a location that cannot be used as a source and it is not on disk. Please do not attempt to use this value again. If you wish to use it again, please write it to disk with `compute_inplace` before writing it to a location."
                end
                sourced(fut, destination)
            end
        end
    end

    # Reset the annotation for this partitioned computation
    set_task(DelayedTask())

    # NOTE: One potential room for optimization is around the fact that
    # whenever we compute something we fully merge it. In fully merging it,
    # we spill it out of memory. Maybe it might be kept in memory and we don't
    # need to set the new source of something being `collect`ed to `Client`.

    fut
end


function partitioned_computation(
    @nospecialize(handler::Function),
    fut::AbstractFuture;
    destination::Location,
    new_source::Union{Location,Function}=NOTHING_LOCATION
)
    if isview(fut)
        throw(ArgumentError("Computing a view (such as a GroupedDataFrame) is not currently supported"))
    end
    if new_source isa Function
        new_source_func = new_source
        new_source = NOTHING_LOCATION
    else
        new_source_func = identity
    end
    partitioned_computation_concrete(handler, convert(Future, fut)::Future, destination, new_source, new_source_func)
end

# Scheduling options
report_schedule = false
encourage_parallelism = false
encourage_parallelism_with_batches = false
exaggurate_size = false
encourage_batched_inner_loop = false
optimize_cpu_cache = false

function get_report_schedule()::Bool
    global report_schedule
    report_schedule
end

function get_encourage_parallelism()::Bool
    global encourage_parallelism
    encourage_parallelism
end

function get_encourage_parallelism_with_batches()::Bool
    global encourage_parallelism_with_batches
    encourage_parallelism_with_batches
end

function get_exaggurate_size()::Bool
    global exaggurate_size
    exaggurate_size
end

function get_encourage_batched_inner_loop()::Bool
    global encourage_batched_inner_loop
    encourage_batched_inner_loop
end

function get_optimize_cpu_cache()::Bool
    global optimize_cpu_cache
    optimize_cpu_cache
end

function configure_scheduling(;kwargs...)
    global report_schedule
    global encourage_parallelism
    global encourage_parallelism_with_batches
    global exaggurate_size
    global encourage_batched_inner_loop
    global optimize_cpu_cache
    kwargs_name::String = get(kwargs, :name, "")
    report_schedule = get(kwargs, :report_schedule, false)::Bool || haskey(kwargs, :name)
    if get(kwargs, :encourage_parallelism, false)::Bool || kwargs_name == "parallelism encouraged"
        encourage_parallelism = true
    end
    if haskey(kwargs, :encourage_parallelism_with_batches) || kwargs_name == "parallelism and batches encouraged"
        encourage_parallelism = true
        encourage_parallelism_with_batches = true
    end
    if get(kwargs, :exaggurate_size, false)::Bool || kwargs_name == "size exaggurated"
        exaggurate_size = true
    end
    if get(kwargs, :encourage_batched_inner_loop, false)::Bool || kwargs_name == "parallelism and batches encouraged"
        encourage_batched_inner_loop = true
    end
    if haskey(kwargs, :optimize_cpu_cache)
        optimize_cpu_cache = kwargs[:optimize_cpu_cache]
    end
    if kwargs_name == "default scheduling"
        encourage_parallelism = false
        encourage_parallelism_with_batches = false
        exaggurate_size = false
        encourage_batched_inner_loop = false
        optimize_cpu_cache = false
    end
end

function send_evaluation(value_id::ValueId, session_id::SessionId)
    # First we ensure that the session is ready. This way, we can get a good
    # estimate of available worker memory before calling evaluate.
    wait_for_session(session_id)

    encourage_parallelism = get_encourage_parallelism()
    encourage_parallelism_with_batches = get_encourage_parallelism_with_batches()
    exaggurate_size = get_exaggurate_size()
    encourage_batched_inner_loop = get_encourage_batched_inner_loop()
    optimize_cpu_cache = get_optimize_cpu_cache()

    # Note that we do not need to check if the session is running here, because
    # `evaluate` will check if the session has failed. If the session is still creating,
    # we will proceed with the eval request, but the client side will wait
    # for the session to be ready when reading from the queue.

    @debug "Sending evaluation request"

    # Get list of the modules used in the code regions here
    record_task_requests::Vector{RecordTaskRequest} = convert(Vector{RecordTaskRequest}, filter(req -> req isa RecordTaskRequest, get_session().pending_requests))
    used_packages = String[]
    for record_task_request in record_task_requests
        for used_module in record_task_request.task.used_modules
            push!(used_packages, used_module)
        end
    end
    used_packages = union(used_packages)  # remove duplicates

    # Submit evaluation request
    !isempty(get_session().organization_id) || error("Organization ID not stored locally for this session")
    !isempty(get_session().cluster_instance_id) || error("Cluster instance ID not stored locally for this session")
    !isnothing(get_session().not_using_modules) || error("Modules not to be used are not stored locally for this session")
    not_using_modules = get_session().not_using_modules
    main_modules = setdiff(get_loaded_packages(),  not_using_modules)
    using_modules = setdiff(used_packages, not_using_modules)
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => value_id,
            "session_id" => session_id,
            "requests" => map(to_jl, get_session().pending_requests),
            "options" => Dict{String,Bool}(
                "report_schedule" => report_schedule,
                "encourage_parallelism" => encourage_parallelism,
                "encourage_parallelism_with_batches" => encourage_parallelism_with_batches,
                "exaggurate_size" => exaggurate_size,
                "encourage_batched_inner_loop" => encourage_batched_inner_loop,
                "optimize_cpu_cache" => optimize_cpu_cache
            ),
            "num_bang_values_issued" => get_num_bang_values_issued(),
            "main_modules" => main_modules,
            "partitioned_using_modules" => using_modules,
            "benchmark" => get(ENV, "BANYAN_BENCHMARK", "0") == "1",
            "worker_memory_used" => get_session().worker_memory_used,
            "resource_id" => get_session().resource_id,
            "organization_id" => get_session().organization_id,
            "cluster_instance_id" => get_session().cluster_instance_id,
            "cluster_name" => get_session().cluster_name,
            "sampling_configs" => sampling_configs_to_jl(get_sampling_configs())
        ),
    )
    if isnothing(response)
        throw(ErrorException("The evaluation request has failed. Please contact support"))
    end

    # Update counters for generating unique values
    set_num_bang_values_issued(response["num_bang_values_issued"])

    # Clear global state and return response
    empty!(get_session().pending_requests)
    response
end

pt_for_replicated(futures::Vector{Future}) = pt(futures[1], Replicated())

compute(fut::AbstractFuture; destroy=Future[]) = compute(convert(Future, fut)::Future; destroy=convert(Vector{Future}, destroy))
function compute(f::Future; destroy=Future[])
    # NOTE: We might be in the middle of an annotation when this is called so
    # we need to avoid partitioned computation (which will reset the task)
    for f_to_destroy in destroy
        if f_to_destroy.value_id == f.value_id
            throw(ArgumentError("Cannot destroy the future being computed"))
        end
        destroy_future(f_to_destroy)
    end

    # Fast case for where the future has not been mutated and isn't stale
    if f.mutated || f.stale
        # We don't need to specify a `source_after` since it should just be
        # `Client()` and the sample won't change at all. Also, we should already
        # have a sample since we are merging it to the client.

        partitioned_computation(pt_for_replicated, f, destination=Client(), new_source=Client(nothing))

        # NOTE: We can't use `new_source=fut->Client(fut.value)` because
        # `new_source` is for locations that require expensive sample collection
        # and so we would only want to compute that location if we really need to
        # use it as a source. Instead in this case, we really just know initially
        # that this is a destination with _some_ value (so we default it to `nothing`)
        # and then right after when we have actually computed, we will set it to the right
        # location using the computed `fut.value`.
        sourced(f, Client(f.value))
    end

    f.value
end

function compute_inplace(fut::AbstractFuture)
    partitioned_computation(pt_for_replicated, fut, destination=Disk())
end


# Make the `offloaded` function on the client side keep looping and 
#     (1) checking receive_next_message and 
#     (2) checking for message[“kind”] == "GATHER" and 
#     (3) `break`ing and `return`ing the value (using `from_jl_string(message["contents"])`) 
#         if value_id == -1
# Make `offloaded` function in Banyan.jl 
#   which calls evaluate passing in a string of bytes 
#   by serializing the given function (just call to_jl_string on it) 
#   and passing it in with the parameter offloaded_function_code
#
# Make `offloaded` function specify 
#     job_id, num_bang_values_issued, main_modules, and benchmark 
#     when calling evaluate (see send_evaluate) and value_id -1
# offloaded(some_func; distributed=true)
# offloaded(some_func, a, b; distributed=true)
function offloaded(given_function::Function, args...; distributed::Bool = false)
    @nospecialize

    # NOTE: no need for wait_for_session here because evaluate for offloaded
    # doesn't need information about memory usage from intiial package loading.

    # Get serialized function
    serialized::String = to_jl_string((given_function, args))

    # Submit evaluation request
    !isempty(get_session().organization_id) || error("Organization ID not stored locally for this session")
    !isempty(get_session().cluster_instance_id) || error("Cluster instance ID not stored locally for this session")
    not_using_modules = get_session().not_using_modules
    main_modules = [m for m in get_loaded_packages() if !(m in not_using_modules)]
    session_id = Banyan.get_session_id()
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => -1,
            "session_id" => session_id,
            "options" => Dict{String,Any}(),
            "num_bang_values_issued" => get_num_bang_values_issued(),
            "main_modules" => main_modules,
            "requests" => [],
            "partitioned_using_modules" => [],
            "benchmark" => get(ENV, "BANYAN_BENCHMARK", "0") == "1",
            "offloaded_function_code" => serialized,
            "distributed" => distributed,
            "worker_memory_used" => get_session().worker_memory_used,
            "resource_id" => get_session().resource_id,
            "organization_id" => get_session().organization_id,
            "cluster_instance_id" => get_session().cluster_instance_id,
            "cluster_name" => get_session().cluster_name,
            "sampling_configs" => sampling_configs_to_jl(get_sampling_configs())
        ),
    )
    if isnothing(response)
        throw(ErrorException("The evaluation request has failed. Please contact support"))
    end

    # We must wait for session because otherwise we will slurp up the session
    # ready message on the gather queue.
    wait_for_session(session_id)

    # job_id = Banyan.get_job_id()
    p = ProgressUnknown("Running offloaded code", spinner=true)
    
    session = get_session()
    gather_queue = gather_queue_url()
    stored_message = nothing
    error_for_main_stuck, error_for_main_stuck_time = nothing, nothing
    partial_gathers = Dict{ValueId,String}()
    while true
        message, error_for_main_stuck = sqs_receive_next_message(gather_queue, p, error_for_main_stuck, error_for_main_stuck_time)
        message_type = message["kind"]::String
        if message_type == "GATHER"
            # Receive gather
            value_id = message["value_id"]::ValueId
            num_chunks = message["num_chunks"]::Int64
            num_remaining_chunks = num_chunks - 1
            
            whole_message_contents = if num_chunks > 1
                partial_messages = Vector{String}(undef, num_chunks)
                partial_messages[message["chunk_idx"]] = message["contents"]
                @sync for i = 1:num_remaining_chunks
                    @async begin
                        partial_message, _ = sqs_receive_next_message(gather_queue, p, nothing, nothing)
                        chunk_idx = partial_message["chunk_idx"]
                        partial_messages[chunk_idx] = message["contents"]
                    end
                end
                join(partial_messages)
            else
                message["contents"]
            end

            if haskey(session.futures_on_client, value_id)
                value = from_jl_string(whole_message_contents)
                f = session.futures_on_client[value_id]::Future
                f.value = value
                # TODO: Update stale/mutated here to avoid costly
                # call to `send_evaluation`
            end

            error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(value_id, contents, error_for_main_stuck, error_for_main_stuck_time)
        elseif (message_type == "EVALUATION_END")
            if message["end"]::Bool == true
                return stored_message
            end
        end
    end
end
    
###############################################################
# Other requests to be sent with request to evaluate a Future #
###############################################################

to_jl(req::RecordTaskRequest) = Dict{String,Any}("type" => "RECORD_TASK", "task" => to_jl(req.task))

to_jl(req::RecordLocationRequest) =
    Dict{String,Any}(
        "type" => "RECORD_LOCATION",
        "value_id" => req.value_id,
        "location" => to_jl(req.location),
    )

to_jl(req::DestroyRequest) = Dict{String,Any}("type" => "DESTROY", "value_id" => req.value_id)

function record_request(@nospecialize(request::Request))
    push!(get_session().pending_requests, request)
end
