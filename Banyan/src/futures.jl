#########
# Types #
#########

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
    new_future = Future(
        nothing,
        value_id,
        false,
        # TODO: Add Size location here if needed
        location.src_name != "None" &&
        location.src_name != "Client" &&
        location.src_name != "Value"
    )
    loc(new_future, None())
    if location.src_name != "None"
        src(new_future, location)
    else
        # For convenience, if a future is constructed with no location to
        # split from, we assume it will be mutated in the next code region
        # and mark it as mutated. This is pretty common since often when
        # we are creating new futures with None location it is as an
        # intermediate variable to store the result of some code region.
        # 
        # Mutation can be specified manually with mutate=true|false in
        # `partition` or implicitly through `Future` constructors
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
    new_future = Future(location)
    new_future.value = value
    new_future
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
    # TODO: Remove mutation if it isn't needed
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

convert(::Type{Future}, value::Any) = Future(value)

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

#################
# Basic Methods #
#################

function compute(fut::AbstractFuture)
    fut = convert(Future, fut)
    job_id = get_job_id()
    job = get_job()

    if fut.mutated
        # Finalize (destroy) all `Future`s that can be destroyed
        GC.gc()

        # Do some final processing on each task to be shipped off
        for req in get_job().pending_requests
            if req isa RecordTaskRequest
                for pa in req.task.pa_union
                    # Materialize all PT compositions that are delayed
                    # NOTE: It might be critical to do this before we finalize since
                    # finalizing will clear all samples and locations 
                    for v in keys(pa.partitions.pt_stacks)
                        delayed_pt_composition = pa.partitions.pt_stacks[v]
                        if delayed_pt_composition isa Function
                            pa.partitions.pt_stacks[v] = delayed_pt_composition()
                        end
                    end

                    # Process each PA before submitting
                    apply_default_constraints!(curr_pa)
                    duplicate_for_batching!(curr_pa)
                end
            elseif req isa DestroyRequest
                # If this value was to be downloaded to or uploaded from the
                # client side, delete the reference to its data
                if fut.value_id in keys(get_job().futures_on_client)
                    delete!(get_job().futures_on_client, fut.value_id)
                end

                # Remove information about the value's location including the
                # sample taken from it
                delete!(get_job().locations, fut.value_id)
            end
        end

        # Send evaluate request
        response = send_evaluation(fut.value_id, job_id)

        # Get queues
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
                    dst(f, None())
                    # TODO: Update stale/mutated here to avoid costly
                    # call to `send_evaluation`
                end
            elseif message_type == "EVALUATION_END"
                @debug "Received evaluation"
                break
            end
        end
    end

    fut.mutated = false
    # TODO: See if there are more cases where you a `compute` call on a future
    # makes it no longer stale
    if get_dst_name(fut) == "None" || get_dst_name(fut) == "Client"
        fut.stale = false
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
    return response
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

    # Mark it as no longer mutated so that its value can be used in calls to
    # `Future(af::AbstractFuture)`. We could be also setting fut.mutated to
    # false for stuff that is not being downloaded to Client and that would
    # prevent redundant evaluations from causing extra work. But this is not
    # something we would expect the user to do (accidentally calling `evaluate`
    # on the same value multiple times). And it's much more useful to ensure
    # that !fut.mutated implies fut.value is the most up-to-date version so
    # that calls to `download` on things like summary statistics (which are
    # more likely to be accidentally duplicated) are fast.

    dst(fut, None())

    fut.value
end

################################
# Methods for setting location #
################################

function src(fut, loc::Location)
    if isnothing(loc.src_name)
        error("Location cannot be used as a source")
    end

    fut::Future = convert(Future, fut)
    fut_location = get_location(fut)
    loc(
        fut,
        Location(
            loc.src_name,
            loc.src_parameters,
            fut_location.dst_name,
            fut_location.dst_parameters,
            max(fut.location.total_memory_usage, loc.total_memory_usage),
        ),
    )
end

function dst(fut, loc::Location)
    if isnothing(loc.dst_name)
        error("Location cannot be used as a destination")
    end

    fut::Future = convert(Future, fut)
    fut_location = get_location(fut.value_id)
    loc(
        fut,
        Location(
            fut_location.src_name,
            fut_location.src_parameters,
            loc.dst_name,
            loc.dst_parameters,
            max(fut.location.total_memory_usage, loc.total_memory_usage),
        ),
    )
end

function loc(fut, location::Location)
    job = get_job()
    fut = convert(Future, fut)
    value_id = fut.value_id

    if location.src_name == "Client" || location.dst_name == "Client"
        job.futures_on_client[value_id] = fut
    else
        # TODO: Set loc of all Futures with Client loc to None at end of
        # evaluate and ensure that this is proper way to handle Client
        delete!(job.futures_on_client, value_id)
    end

    job.locations[value_id] = location
    record_request(RecordLocationRequest(value_id, location))
end

function loc(futs...)
    futs = futs .|> obj->convert(Future, obj)
    maxindfuts = argmax([
        get_location(f).total_memory_usage
        for f in futs
    ])
    for fut in futs
        loc(
            fut,
            get_location(futs[maxindfuts]),
        )
    end
end

# NOTE: The below operations (mem and val) should rarely be used if every. We
# should probably even remove them at some point. Memory usage of each sample
# is automatically detected and stored. If you want to make a future have Value
# location type, simply use the `Future` constructor and pass your value in.

function mem(fut, estimated_total_memory_usage::Integer)
    fut = convert(Future, fut)
    location = get_location(fut)
    location.total_memory_usage = estimated_total_memory_usage
    record_request(RecordLocationRequest(fut.value_id, location))
end

mem(fut, n::Integer, ty::DataType) = mem(fut, n * sizeof(ty))
mem(fut) = mem(fut, sizeof(convert(Future, fut).value))

function mem(futs...)
    for fut in futs
        mem(
            fut,
            maximum([
                begin
                    get_location(f).total_memory_usage
                end
                for f in futs
            ]),
        )
    end
end

val(fut) = loc(fut, Value(convert(Future, fut).value))

################################
# Methods for getting location #
################################

get_src_name(fut) = get_location(fut).src_name
get_dst_name(fut) = get_location(fut).dst_name
get_src_parameters(fut) = get_location(fut).src_parameters
get_dst_parameters(fut) = get_location(fut).dst_parameters
