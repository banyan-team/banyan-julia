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
    message::Dict{String,Any},
    error_for_main_stuck::Union{Nothing,String},
    error_for_main_stuck_time::Union{Nothing,DateTime}
)::Tuple{Union{Nothing,String},Union{Nothing,DateTime}}
    value_id = message["value_id"]::ValueId
    if value_id == "-2" && isnothing(error_for_main_stuck_time)
        error_for_main_stuck_msg::String = from_jl_value_contents(message["contents"]::String)
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
    if !isnothing(error_for_main_stuck) && !isnothing(error_for_main_stuck_time) && (Dates.now() - error_for_main_stuck_time) > Second(30)
        println(error_for_main_stuck)
        @warn "The above error occurred on some workers but other workers are still running. Please interrupt and end the session unless you expect that a lot of logs are being returned."
        error_for_main_stuck = nothing
    end
    error_for_main_stuck
end

function partitioned_computation(
    handler::Function,
    fut::AbstractFuture;
    destination::Location,
    new_source::Union{Location,Function}=NOTHING_LOCATION
)
    if isview(fut)
        error("Computing a view (such as a GroupedDataFrame) is not currently supported")
    end
    if new_source isa Function
        new_source_func = new_source
        new_source = NOTHING_LOCATION
    else
        new_source_func = identity
    end
    partitioned_computation(handler, convert(Future, fut)::Future, destination, new_source, new_source_func)
end

function partitioned_computation(
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

    global sessions

    session_id = get_session_id()
    session = get_session()
    resource_id = session.resource_id

    destination_dst_name::String = destination.dst_name
    if fut.mutated || (destination_dst_name == "Client" && fut.stale) || destination_dst_name == "Remote"
        # TODO: Check to ensure that `fut` is annotated
        # This creates an empty final task that ensures that the future
        # will be scheduled to get sent to its destination.
        @time begin
        @time begin
        destined(fut, destination)
        mutated(fut)
        partitioned_with(scaled=fut) do
            handler(fut)
        end
        @partitioned fut begin end
        println("Time for creating final task:")
        end

        # Get all tasks to be recorded in this call to `compute`
        println("Time for getting tasks:")
        tasks::Vector{DelayedTask} = @time DelayedTask[req.task for req in session.pending_requests if req isa RecordTaskRequest]
        tasks_reverse::Vector{DelayedTask} = reverse(tasks)
        @show length(tasks)

        # Call `partitioned_using_func`s in 2 passes - forwards and backwards.
        # This allows sample properties to propagate in both directions. We
        # must also make sure to apply mutations in each task appropriately.
        @time begin
        for t in tasks_reverse
            apply_mutation(invert(t.mutation))
        end
        for t in tasks
            set_task(t)
            if !isnothing(t.partitioned_using_func)
                partitioned_using_func = t.partitioned_using_func
                partitioned_using_func()
            end
            apply_mutation(t.mutation)
        end
        for t in tasks_reverse
            set_task(t)
            apply_mutation(invert(t.mutation))
            if !isnothing(t.partitioned_using_func)
                partitioned_using_func = t.partitioned_using_func
                partitioned_using_func()
            end
        end
        println("Applying mutation time:")
        end

        # Do further processing on tasks now that all samples have been
        # computed and sample properties have been set up to share references
        # as needed to prevent expensive redundant computation of sample
        # properties like divisions
        @time begin
        for t::DelayedTask in tasks
            apply_mutation(t.mutation)
            
            # Call `partitioned_with_func` to create additional PAs for each task
            set_task(t)
            if !isnothing(t.partitioned_with_func)
                partitioned_with_func::Function = t.partitioned_with_func
                partitioned_with_func()
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
        println("Time for applying PAs:")
        end

        # Switch back to a new task for next code region
        finish_task()

        # Iterate through tasks for further processing before recording them
        @time begin
        for t::DelayedTask in tasks
            # Apply defaults to PAs
            for pa::PartitionAnnotation in t.pa_union
                apply_default_constraints!(pa)
                duplicate_for_batching!(pa)
            end

            # Destroy all closures so that all references to `Future`s are dropped
            t.partitioned_using_func = nothing
            t.partitioned_with_func = nothing

            # Handle 
            empty!(t.mutation) # Drop references to `Future`s here as well

            # @show statements for displaying info about each task
            if isinvestigating()[:tasks]
                @show t.memory_usage
                @show t.inputs
                @show t.outputs
                @show t.code
                @show t.value_names
                @show t.effects
                @show t.pa_union
            end
        end
        println("Time for further processing")
        end

        # Finalize (destroy) all `Future`s that can be destroyed
        println("Time for destroying stuff with GC.gc():")
        @time GC.gc()
    
        # Destroy everything that is to be destroyed in this task
        for req in session.pending_requests
            # Don't destroy stuff where a `DestroyRequest` was produced just
            # because of a `mutated(old, new)`
            if req isa DestroyRequest
                is_mutated = false
                req_value_id::ValueId = req.value_id
                for t in tasks
                    if req_value_id in values(t.mutation)
                        is_mutated = true
                    end
                end
                if !is_mutated
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
                    @show req.value_id
                    delete!(session.locations, req_value_id)
                end
            end
        end
        println("Time for preparing tasks:")
        end # end of preparing tasks
    
        # Send evaluation request
        @time begin
        is_merged_to_disk::Bool = false
        try
            response = send_evaluation(fut.value_id, session_id)
            is_merged_to_disk = response["is_merged_to_disk"]::Bool
        catch
            end_session(failed=true)
            rethrow()
        end
        println("Time for send_evaluation:")
        end
    
        # Get queues for moving data between client and cluster
        println("get_scatter_queue")
        scatter_queue = @time get_scatter_queue(resource_id)
        println("get_gather_queue")
        gather_queue = @time get_gather_queue(resource_id)
    
        # Read instructions from gather queue
        # @info "Computing result with ID $(fut.value_id)"
        println("Wait for result")
        @time begin
        @debug "Waiting on running session $session_id, listening on $gather_queue, and computing value with ID $(fut.value_id)"
        p = ProgressUnknown("Computing value with ID $(fut.value_id)", spinner=true)
        println("get_session_status")
        session_status::String = @time get_session_status(session_id)
        if session_status != "running"
            wait_for_session(session_id)
        end
        error_for_main_stuck::Union{Nothing,String} = nothing
        error_for_main_stuck_time::Union{Nothing,DateTime} = nothing
        while true
            # TODO: Use to_jl_value and from_jl_value to support Client
            println("receive_next_message")
            message, error_for_main_stuck = @time receive_next_message(gather_queue, p, error_for_main_stuck, error_for_main_stuck_time)
            message_type::String = message["kind"]
            if message_type == "SCATTER_REQUEST"
                # Send scatter
                value_id = message["value_id"]::ValueId
                haskey(session.futures_on_client, value_id) || error("Expected future to be stored on client side")
                f = session.futures_on_client[value_id]::Future
                # @debug "Received scatter request for value with ID $value_id and value $(f.value) with location $(get_location(f))"
                println("send_message")
                @time send_message(
                    scatter_queue,
                    JSON.json(
                        Dict{String,Any}(
                            "value_id" => value_id,
                            "contents" => to_jl_value_contents(f.value)
                        ),
                    ),
                )
                # sourced(f, None())
                # TODO: Update stale/mutated here to avoid costly
                # call to `send_evaluation`
            elseif message_type == "GATHER"
                # Receive gather
                value_id = message["value_id"]::ValueId
                # @debug "Received gather request for $value_id"
                if haskey(session.futures_on_client, value_id)
                    value = from_jl_value_contents(message["contents"]::String)
                    f = session.futures_on_client[value_id]::Future
                    f.value = value
                    # @debug "Received $(f.value)"
                    # TODO: Update stale/mutated here to avoid costly
                    # call to `send_evaluation`
                end
                error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(message, error_for_main_stuck, error_for_main_stuck_time)
            elseif message_type == "EVALUATION_END"
                # @debug "End of evaluation"
                if message["end"]::Bool == true
                    break
                end
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
        if is_merged_to_disk
            sourced(fut, Disk())
        else
            # TODO: If not still merged to disk, we need to lazily set the location source to something else
            if !isnothing(new_source)
                sourced(fut, new_source)
            elseif new_source_func !== identity
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

        # Reset the location destination to its default. This is where we
        # update the location destination.
        destined(fut, None())
    end

    # Reset the annotation for this partitioned computation
    set_task(DelayedTask())

    # NOTE: One potential room for optimization is around the fact that
    # whenever we compute something we fully merge it. In fully merging it,
    # we spill it out of memory. Maybe it might be kept in memory and we don't
    # need to set the new source of something being `collect`ed to `Client`.
    @show sample(fut)

    fut
end

# Scheduling options
report_schedule = false
encourage_parallelism = false
encourage_parallelism_with_batches = false
exaggurate_size = false

function configure_scheduling(;kwargs...)
    global report_schedule
    global encourage_parallelism
    global encourage_parallelism_with_batches
    global exaggurate_size
    report_schedule = get(kwargs, :report_schedule, false) || haskey(kwargs, :name)
    if get(kwargs, :encourage_parallelism, false) || get(kwargs, :name, "") == "parallelism encouraged"
        encourage_parallelism = true
    end
    if haskey(kwargs, :encourage_parallelism_with_batches) || get(kwargs, :name, "") == "parallelism and batches encouraged"
        encourage_parallelism = true
        encourage_parallelism_with_batches = true
    end
    if get(kwargs, :exaggurate_size, false) || get(kwargs, :name, "") == "size exaggurated"
        exaggurate_size = true
    end
    if get(kwargs, :name, "") == "default scheduling"
        encourage_parallelism = false
        encourage_parallelism_with_batches = false
        exaggurate_size = false
    end
end

function send_evaluation(value_id::ValueId, session_id::SessionId)
    global encourage_parallelism
    global encourage_parallelism_with_batches
    global exaggurate_size

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
    @show map(to_jl, get_session().pending_requests)
    @show length(get_session().pending_requests)
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => value_id,
            "session_id" => session_id,
            "requests" => map(to_jl, get_session().pending_requests),
            "options" => Dict(
                "report_schedule" => report_schedule,
                "encourage_parallelism" => encourage_parallelism,
                "encourage_parallelism_with_batches" => encourage_parallelism_with_batches,
                "exaggurate_size" => exaggurate_size
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

compute(fut::AbstractFuture) = compute(convert(Future, fut)::Future)
function compute(f::Future)

    # NOTE: We might be in the middle of an annotation when this is called so
    # we need to avoid partitioned computation (which will reset the task)

    # # Fast case for where the future has not been mutated and isn't stale
    if f.mutated || f.stale
        # # This function collects the given future on the client side
        
        # # NOTE: If the value was already replicated and has never been written to disk, then this
        # # might send it to the client and never allow the value to be used again since it hasn't been saved.
        # # By computing it now, we can ensure that its saved to disk. This is one of the things we should address when we
        # # clean up locations.
        # compute(fut)
        
        # # Set the future's destination location to Client
        # destined(fut, Client())
        # mutated(fut)

        # pt(fut, Replicated())
        # @partitioned fut begin
        #     # This code region is empty but it ensures that something is run
        #     # and so the data is partitioned and then re-merged back up to its new
        #     # destination location, the client
        # end

        # # Evaluate the future so that its value is downloaded to the client
        # compute(fut)
        # destined(fut, None())
        # @show fut.value
        # fut.value

        # We don't need to specify a `source_after` since it should just be
        # `Client()` and the sample won't change at all. Also, we should already
        # have a sample since we are merging it to the client.

        partitioned_computation(f, destination=Client(), new_source=Client(nothing)) do fut::Future
            pt(fut, Replicated())
        end

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
    partitioned_computation(fut, destination=Disk()) do f::Future
        pt(f, Replicated())
    end
end


# Make the `offloaded` function on the client side keep looping and 
#     (1) checking receive_next_message and 
#     (2) checking for message[“kind”] == "GATHER" and 
#     (3) `break`ing and `return`ing the value (using `from_jl_value_contents(message["contents"])`) 
#         if value_id == -1
# Make `offloaded` function in Banyan.jl 
#   which calls evaluate passing in a string of bytes 
#   by serializing the given function (just call to_jl_value_contents on it) 
#   and passing it in with the parameter offloaded_function_code
#
# Make `offloaded` function specify 
#     job_id, num_bang_values_issued, main_modules, and benchmark 
#     when calling evaluate (see send_evaluate) and value_id -1
# offloaded(some_func; distributed=true)
# offloaded(some_func, a, b; distributed=true)
function offloaded(given_function::Function, args...; distributed::Bool = false)
    @nospecialize

    # Get serialized function
    serialized::String = to_jl_value_contents((given_function, args))

    # Submit evaluation request
    println("Sending request and getting response")
    !isempty(get_session().organization_id) || error("Organization ID not stored locally for this session")
    !isempty(get_session().cluster_instance_id) || error("Cluster instance ID not stored locally for this session")
    not_using_modules = get_session().not_using_modules
    main_modules = [m for m in get_loaded_packages() if !(m in not_using_modules)]
    response = @time send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => -1,
            "session_id" => Banyan.get_session_id(),
            "options" => Dict( ),
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
        ),
    )
    if isnothing(response)
        throw(ErrorException("The evaluation request has failed. Please contact support"))
    end

    # job_id = Banyan.get_job_id()
    p = ProgressUnknown("Running offloaded code", spinner=true)
    
    session = get_session()
    gather_queue = get_gather_queue(session.resource_id)
    stored_message = nothing
    error_for_main_stuck, error_for_main_stuck_time = nothing, nothing
    while true
        message, error_for_main_stuck = @time receive_next_message(gather_queue, p, error_for_main_stuck, error_for_main_stuck_time)
        @show message
        message_type = message["kind"]::String
        if (message_type == "GATHER")
            value_id = message["value_id"]::Int64
            if (value_id == "-1")
                memory_used = message["worker_memory_used"]::Int64
                get_session().worker_memory_used = get_session().worker_memory_used + memory_used
                stored_message = from_jl_value_contents(message["contents"]::String)
            end
            error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(message, error_for_main_stuck, error_for_main_stuck_time)
        elseif (message_type == "EVALUATION_END")
            return stored_message
        end
    end
end
    
###############################################################
# Other requests to be sent with request to evaluate a Future #
###############################################################

struct RecordTaskRequest
    task::DelayedTask
end

struct RecordLocationRequest
    value_id::ValueId
    location::Location
end

struct DestroyRequest
    value_id::ValueId
end

const Request = Union{RecordTaskRequest,RecordLocationRequest,DestroyRequest}

to_jl(req::RecordTaskRequest) = Dict("type" => "RECORD_TASK", "task" => to_jl(req.task))

to_jl(req::RecordLocationRequest) =
    Dict(
        "type" => "RECORD_LOCATION",
        "value_id" => req.value_id,
        "location" => to_jl(req.location),
    )

to_jl(req::DestroyRequest) = Dict("type" => "DESTROY", "value_id" => req.value_id)

function record_request(@nospecialize(request::Request))
    push!(get_session().pending_requests, request)
end
