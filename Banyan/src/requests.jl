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

function partitioned_computation(fut::AbstractFuture; destination, new_source=nothing)
    if isview(fut)
        error("Computing a view (such as a GroupedDataFrame) is not currently supported")
    end

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

    global jobs

    fut = convert(Future, fut)
    job_id = get_job_id()
    job = get_job()

    @show fut.value_id
    @show fut.stale
    @show destination
    @show fut.mutated

    if fut.mutated || (destination.dst_name == "Client" && fut.stale)
        # TODO: Check to ensure that `fut` is annotated
        # This creates an empty final task that ensures that the future
        # will be scheduled to get sent to its destination.
        destined(fut, destination)
        mutated(fut)
        @partitioned fut begin end

        # Get all tasks to be recorded in this call to `compute`
        tasks = [req.task for req in job.pending_requests if req isa RecordTaskRequest]

        # Call `partitioned_using_func`s in 2 passes - forwards and backwards.
        # This allows sample properties to propagate in both directions. We
        # must also make sure to apply mutations in each task appropriately.
        for t in tasks
            if is_debug_on()
	            @show t.mutation
                @show t.effects
            end
        end
        for t in Iterators.reverse(tasks)
            apply_mutation(invert(t.mutation))
        end
        for t in tasks
            if !isnothing(t.partitioned_using_func)
                t.partitioned_using_func()
            end
            apply_mutation(t.mutation)
        end
        for t in Iterators.reverse(tasks)
            apply_mutation(invert(t.mutation))
            if !isnothing(t.partitioned_using_func)
                t.partitioned_using_func()
            end
        end

        # Do further processing on tasks now that all samples have been
        # computed and sample properties have been set up to share references
        # as needed to prevent expensive redundant computation of sample
        # properties like divisions
        for (i, t) in enumerate(tasks)
            apply_mutation(t.mutation)
            
            # Call `partitioned_with_func` to create additional PAs for each task
            set_task(t)
            if !isnothing(t.partitioned_with_func)
                t.partitioned_with_func()
            end

            # Cascade PAs backwards. In other words, if as we go from first to
            # last PA we come across one that's annotating a value not
            # annotated in a previous PA, we copy over the annotation (the
            # assigned PT stack) to the previous PA.
            for (j, pa) in enumerate(t.pa_union)
                # For each PA in this PA union for this task, we consider the
                # PAs before it
                for previous_pa in Iterators.reverse(t.pa_union[1:j-1])
                    for value_id in keys(pa.partitions.pt_stacks)
                        # Check if there is a previous PA where this value
                        # does not have a PT.
                        if !(value_id in keys(previous_pa.partitions.pt_stacks))
                            # Cascade the PT composition backwards
                            previous_pa.partitions.pt_stacks[value_id] =
                                deepcopy(pa.partitions.pt_stacks[value_id])

                            # Cascade backwards all constraints that mention the
                            # value. NOTE: If this is not desired, users should
                            # be explicit and assign different PT compositions for
                            # different values.
                            for constraint in pa.constraints.constraints
                                # Determine whether we should copy over this constraint
                                copy_constraint = false
                                if constraint isa PartitioningConstraintOverGroup
                                    for arg in constraint.args
                                        if arg isa PartitionTypeReference && first(arg) == value_id
                                            copy_constraint = true
                                        end
                                    end
                                elseif constraint isa PartitioningConstraintOverGroups
                                    for arg in constraint.args
                                        for subarg in arg
                                            if subarg isa PartitionTypeReference && first(subarg) == value_id
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

        # for t in tasks
        #     # Apply defaults to PAs
        #     for pa in t.pa_union
        #         @show pa
        #     end
        # end

        # Iterate through tasks for further processing before recording them
        for t in tasks
            if is_debug_on()
		        @show t.code
                @show t.value_names
                @show t.mutation
                @show t.effects
            end
            # Apply defaults to PAs
            for pa in t.pa_union
                apply_default_constraints!(pa)
                duplicate_for_batching!(pa)
                if is_debug_on()
		            @show pa
                end
            end

            # Destroy all closures so that all references to `Future`s are dropped
            t.partitioned_using_func = nothing
            t.partitioned_with_func = nothing

            # Handle 
            empty!(t.mutation) # Drop references to `Future`s here as well
        end

        # Finalize (destroy) all `Future`s that can be destroyed
        GC.gc()
    
        # Destroy everything that is to be destroyed in this task
        for req in job.pending_requests
            # Don't destroy stuff where a `DestroyRequest` was produced just
            # because of a `mutated(old, new)`
            if req isa DestroyRequest && !any(req.value_id in values(t.mutation) for t in tasks)
                # If this value was to be downloaded to or uploaded from the
                # client side, delete the reference to its data
                if req.value_id in keys(job.futures_on_client)
                    delete!(job.futures_on_client, req.value_id)
                end
    
                # Remove information about the value's location including the
                # sample taken from it
                delete!(job.locations, req.value_id)
            end
        end
    
        # Send evaluation request
        is_merged_to_disk = false
        try
            response = send_evaluation(fut.value_id, job_id)
            is_merged_to_disk = response["is_merged_to_disk"]
        catch
            jobs[job_id].current_status = "failed"
            rethrow()
        end
    
        # Get queues for moving data between client and cluster
        scatter_queue = get_scatter_queue(job_id)
        gather_queue = get_gather_queue(job_id)
    
        # Read instructions from gather queue
        # println("job id: ", job_id)
        # print("LISTENING ON: ", gather_queue)
        @debug "Waiting on running job $job_id"
        println("Waiting on running job $job_id and computing value with ID " * fut.value_id)
        while true
            # TODO: Use to_jl_value and from_jl_value to support Client
            message = receive_next_message(gather_queue)
            @show message
            message_type = message["kind"]
            if message_type == "SCATTER_REQUEST"
                @debug "Received scatter request"
                # Send scatter
                value_id = message["value_id"]
                f = job.futures_on_client[value_id]
                send_message(
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
                @debug "Received gather request"
                # Receive gather
                value_id = message["value_id"]
                if value_id in keys(job.futures_on_client)
                    value = from_jl_value_contents(message["contents"])
                    f::Future = job.futures_on_client[value_id]
                    f.value = value
                    # TODO: Update stale/mutated here to avoid costly
                    # call to `send_evaluation`
                    @debug value
                end
            elseif message_type == "EVALUATION_END"
                @debug "Received evaluation"
                if message["end"] == true
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
        if is_merged_to_disk
            sourced(fut, Disk())
        else
            # TODO: If not still merged to disk, we need to lazily set the location source to something else
            if !isnothing(new_source)
                sourced(fut, new_source)
            else
                sourced(fut, destination)
            end
        end

        # Reset the location destination to its default. This is where we
        # update the location destination.
        destined(fut, None())
    end

    # Reset the annotation for this partitioned computation
    set_task(DelayedTask())

    fut
end

# Scheduling options
report_schedule = false
encourage_parallelism = false
encourage_parallelism_with_batches = false

function configure_scheduling(;kwargs...)
    global report_schedule
    global encourage_parallelism
    global encourage_parallelism_with_batches
    report_schedule = get(kwargs, :report_schedule, false) || haskey(kwargs, :name)
    if get(kwargs, :encourage_parallelism, false) || get(kwargs, :name, "") == "parallelism encouraged"
        encourage_parallelism = kwargs[:encourage_parallelism]
    end
    if haskey(kwargs, :encourage_parallelism_with_batches) || get(kwargs, :name, "") == "parallelism and batches encouraged"
        encourage_parallelism_with_batches = kwargs[:encourage_parallelism_with_batches]
    end
end

function send_evaluation(value_id::ValueId, job_id::JobId)
    global encourage_parallelism
    global encourage_parallelism_with_batches

    @debug "Sending evaluation request"

    # Submit evaluation request
    println("Submitting evaluation request")
    @show value_id
    @show [to_jl(req) for req in get_job().pending_requests]
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => value_id,
            "job_id" => job_id,
            "requests" => [to_jl(req) for req in get_job().pending_requests],
            "options" => Dict(
                "report_schedule" => report_schedule,
                "encourage_parallelism" => encourage_parallelism,
                "encourage_parallelism_with_batches" => encourage_parallelism_with_batches
            ),
            "num_bang_values_issued" => get_num_bang_values_issued(),
            "packages" => get_loaded_packages()
        ),
    )

    @show response

    # Update counters for generating unique values
    set_num_bang_values_issued(response["num_bang_values_issued"])

    # Clear global state and return response
    empty!(get_job().pending_requests)
    response
end

function Base.collect(fut::AbstractFuture)
    fut = convert(Future, fut)

    # NOTE: We might be in the middle of an annotation when this is called so
    # we need to avoid partitioned computation (which will reset the task)

    # # Fast case for where the future has not been mutated and isn't stale
    # @show fut.mutated
    # @show fut.stale
    if !fut.mutated && !fut.stale
        return fut.value
    end

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

    pt(fut, Replicated())
    partitioned_computation(fut, destination=Client())

    fut.value
end

function write_to_disk(fut::AbstractFuture)
    fut = convert(Future, fut)

    pt(fut, Replicated())
    partitioned_computation(fut, destination=Disk())
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

function record_request(request::Request)
    push!(get_job().pending_requests, request)
end
