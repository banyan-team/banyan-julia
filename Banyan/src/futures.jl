###########
# Futures #
###########

abstract type AbstractFuture end

mutable struct Future <: AbstractFuture
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool
end

"""
    Future()
    Future(value::Any)
    Future(location::Location)
    Future(; kwargs...)

Constructs a new future, representing a value that has not yet been evaluated.
"""
function Future(location::Location = None())
    # Generate new value id
    value_id = generate_value_id()

    # Create new Future and assign a location to it
    new_future = Future(nothing, value_id, false, false)
    src(new_future, location)
    dst(new_future, None())

    # TODO: Add Size location here if needed
    # Handle locations that have an associated value
    if location.src_name in ["None", "Client", "Value"]
        new_future.value = location.sample.value
        new_future.stale = false
    end
    
    # For convenience, if a future is constructed with no location to
    # split from, we assume it will be mutated in the next code region
    # and mark it as mutated. This is pretty common since often when
    # we are creating new futures with None location it is as an
    # intermediate variable to store the result of some code region.
    # 
    # Mutation can also be specified manually with mutate=true|false in
    # `partition` or implicitly through `Future` constructors
    if location.src_name == "None"
        mut(future)
    end

    # Create finalizer and register
    finalizer(new_future) do fut
        record_request(DestroyRequest(value_id))
    end

    new_future
end

function Future(value::Any)
    location = if Base.summarysize(value) ≤ 4 * 1024
        Value(value)
    else
        Client(value)
    end

    # Create future, store value, and return
    Future(location)
end

"""
    Future(future::AbstractFuture)

Constructs a future from a future that was already created.

If the given future has not had its value mutated (meaning that the value
stored with it here on the client is the most up-to-date version of it), we use
its value to construct a new future from a copy of the value.

However, if the future has been mutated by some code region that has already
been recorded, we construct a new future with location `None` and mark it as
mutated. This is because presumably in the case that we _can't_ copy over the
given future, we would want to assign to it in the upcoming code region where
it's going to be used.
"""
function Future(fut::AbstractFuture, mutation::Function=identity)
    fut = convert(Future, fut)
    if !fut.stale
        Future(
            copy(mutation(fut.value)),
            generate_value_id(),
            # If the future is not stale, it is not mutated in a way where
            # a further `compute` is needed. So we can just copy its value.
            false,
            false
        )
    else
        Future()
    end
end

function Future(;mutate_from::AbstractFuture)
    fut = Future()
    mut(mutate_from, fut)
    fut
end

convert(::Type{Future}, value::Any) = Future(value)

get_location(fut::AbstractFuture) = get_job().locations[convert(Future, fut).value_id]
get_location(value_id::ValueId) = get_job().locations[value_id]
get_future(value_id::ValueId) = get_job().futures_on_client[value_id]

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

function compute(fut::AbstractFuture)
    fut = convert(Future, fut)
    job_id = get_job_id()
    job = get_job()

    if fut.mutated
        # Get all tasks to be recorded in this call to `compute`
        tasks = [req.task for req in job.pending_requests if req isa RecordTaskRequest]

        # Call `partitioned_using_func`s in 2 passes - forwards and backwards.
        # This allows sample properties to propagate in both directions. We
        # must also make sure to apply mutations in each task appropriately.
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
            for pa in t.pa_union
                for previous_pa in Iterators.reverse(tasks[1:i-1])
                    for value_id in keys(pa.partitions.pt_stacks)
                        if !(value_id in keys(previous_pa.partitions.pt_stacks))
                            previous_pa.partitions.pt_stacks[value_id] =
                                pa.partitions.pt_stacks[value_id]
                        end
                    end
                end
            end
        end

        # Iterate through tasks for further processing before recording them
        for t in tasks
            # Apply defaults to PAs
            for pa in t.pa_union
                apply_default_constraints!(pa)
                duplicate_for_batching!(pa)
            end

            # Destroy all closures so that all references to `Future`s are dropped
            t.partitioned_using_func = nothing
            t.partitioned_with_func = nothing

            # Handle mutation
            clear(t.mutation) # Drop references to `Future`s here as well
        end

        # Finalize (destroy) all `Future`s that can be destroyed
        GC.gc()
    
        # Destroy everything that is to be destroyed in this task
        for req in job.pending_requests
            # Don't destroy stuff where a `DestroyRequest` was produced just
            # because of a `mut(old, new)`
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
        response = send_evaluation(fut.value_id, job_id)
    
        # Get queues for moving data between client and cluster
        scatter_queue = get_scatter_queue(job_id)
        gather_queue = get_gather_queue(job_id)
    
        # Read instructions from gather queue
        # println("job id: ", job_id)
        # print("LISTENING ON: ", gather_queue)
        @debug "Waiting on running job $job_id"
        while true
            # TODO: Use to_jl_value and from_jl_value to support Client
            message = receive_next_message(gather_queue)
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
                src(f, None())
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
                end
            elseif message_type == "EVALUATION_END"
                @debug "Received evaluation"
                break
            end
        end

        # Update `mutated` and `stale` for the future that is being evaluated
        fut.mutated = false
        # TODO: See if there are more cases where you a `compute` call on a future
        # makes it no longer stale
        if get_dst_name(fut) == "Client"
            fut.stale = false
        end
    end

    fut
end

function send_evaluation(value_id::ValueId, job_id::JobId)
    @debug "Sending evaluation request"

    # Submit evaluation request
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => value_id,
            "job_id" => job_id,
            "requests" => [to_jl(req) for req in get_job().pending_requests]
        ),
    )

    # Clear global state and return response
    empty!(get_job().pending_requests)
    response
end

function collect(fut::AbstractFuture)
    # Set the future's destination location to Client
    dst(fut, Client())

    partition(fut, Partitioned(), mutate=true)
    @partitioned fut begin
        # This code region is empty but it ensures that something is run
        # and so the data is partitioned and then re-merged back up to its new
        # destination location, the client
    end

    # Evaluate the future so that its value is downloaded to the client
    compute(fut)
    dst(fut, None())
    fut.value
end

###############################################################
# Other requests to be sent with request to evaluate a Future #
###############################################################

const Request = Union{RecordTaskRequest,RecordLocationRequest,DestroyRequest}

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
