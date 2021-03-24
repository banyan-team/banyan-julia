using JSON
using Serialization

#########
# Types #
#########
global future_count = 0
mutable struct Future
    value::Any
    value_id::ValueId
    mutated::Bool
    location::Location

    # NOTE: This should not be called in application code; use `fut` instead.
    function Future(value = nothing)
        global future_count
        future_count += 1
        # Generate new value id
        value_id = create_value_id()

        # Create new Future and add to futures dictionary if lt is Client
        new_future = new(value, value_id, false, None())
        loc(new_future, None())

        # Create finalizer and register
        finalizer(new_future) do fut
            global futures
            if fut.value_id in keys(futures)
                delete!(futures, fut.value_id)
            end
            record_request(DestroyRequest(value_id))
            #println(value_id)
        end

        new_future
    end
end

# This is the method that should generally be used for constructing Futures.
# If the input is already a Future in some form (e.g., it is a BanyanArray
# containing a Future or a Future itself), this will simply return that future
# that already exists. Otherwise, a new Future will be constructed with
# location None. Then, the location can be set with `loc`/`src`/`dst`.
function future(value = nothing)::Future
    if value isa Future
        value
    else
        return Future(value)
    end
end

################
# Global State #
################

# Futures that have location as "Client"
global futures = Dict{ValueId,Future}()

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

function evaluate(fut, job_id::JobId)
    # Finalize all Futures that can be destroyed
    println("IN EVALUATE")
    global pending_requests
    global future_count
    GC.gc()
    #all_values = Set()
    #destroyed_values = Set()
    #for req in pending_requests
    #    if typeof(req) == RecordTaskRequest
    #        for key in keys(req.task.value_names)
    #            push!(all_values, key)
    #        end
    #    elseif typeof(req) == DestroyRequest
    #        push!(destroyed_values, req.value_id)
    #    end
    #end
    #println("futures created ", future_count)
    #println("all in task ", length(all_values))
    #println("destroyed ", length(destroyed_values))
    #println("intersection ", length(intersect(all_values, destroyed_values)))

    global futures
    fut = future(fut)

    if fut.mutated
        println("EVALUATE getting sent")
        fut.mutated = false

        # Send evaluate request
        response = send_evaluation(fut.value_id, job_id)

        # Get queues
        scatter_queue = get_scatter_queue(job_id)
        gather_queue = get_gather_queue(job_id)

        # Read instructions from gather queue
        println("job id: ", job_id)
        print("LISTENING ON: ", gather_queue)
        while true
            message = receive_next_message(gather_queue)
            message_type = message["kind"]
            if message_type == "SCATTER_REQUEST"
                # Send scatter
                value_id = message["value_id"]
                buf = IOBuffer()
                serialize(buf, if value_id in keys(futures)
                    futures[value_id].value
                else
                    nothing
                end)
                value = take!(buf)
                send_message(
                    scatter_queue,
                    JSON.json(Dict{String,Any}(
                        "value_id" => value_id,
                        "value" => value,
                    )),
                )
            elseif message_type == "GATHER"
                # Receive gather
                value_id = message["value_id"]
                value = deserialize(IOBuffer(convert(
                    Array{UInt8},
                    message["value"],
                )))
                setfield!(futures[value_id], :value, value)
                # Mark other futures that have been gathered as not mut
                #   so that we can avoid unnecessarily making a call to AWS
                if value_id in keys(futures)
                    futures[value_id].mutated = false
                end
            elseif message_type == "EVALUATION_END"
                break
            end
        end
    end

    return getfield(fut, :value)
end

evaluate(fut, job::Job) = evaluate(fut, job.job_id)
evaluate(fut) = evaluate(fut, get_job_id())

function send_evaluation(value_id::ValueId, job_id::JobId)
    # TODO: Serialize requests_list to send
    global pending_requests
    #print("SENDING NOW", requests_list)
    response = send_request_get_response(
        :evaluate,
        Dict{String,Any}(
            "value_id" => value_id,
            "job_id" => job_id,
            "requests" => [to_jl(req) for req in pending_requests],
            # TODO: Add locations and deleted
            # TODO: Remove requests
            # TODO: Modify requests
        ),
    )
    empty!(pending_requests)
    return response
end

################################
# Methods for Setting Location #
################################

function src(fut, loc::Location)
    global futures
    fut = future(fut)

    if loc.src_name == "Client"
        futures[value_id] = fut
    end

    fut.location.src_name = loc.src_name
    fut.location.src_parameters = loc.src_parameters
    record_request(RecordLocationRequest(fut.value_id, fut.location))
end

function dst(fut, loc::Location)
    global futures
    fut = future(fut)

    if loc.dst_name == "Client"
        futures[value_id] = fut
    end

    fut.location.dst_name = loc.dst_name
    fut.location.dst_parameters = loc.dst_parameters
    record_request(RecordLocationRequest(fut.value_id, fut.location))
end

function loc(fut, loc::Location)
    global futures
    fut = future(fut)

    if loc.src_name == "Client" || loc.dst_name == "Client"
        futures[value_id] = fut
    end

    fut.location = loc
    record_request(RecordLocationRequest(fut.value_id, fut.location))
end

function mem(fut, estimated_total_memory_usage::Integer)
    fut = future(fut)
    fut.location.total_memory_usage = estimated_total_memory_usage
    record_request(RecordLocationRequest(fut.value_id, fut.location))
end

mem(fut, n::Integer, ty::DataType) = mem(fut, n * sizeof(ty))
mem(fut) = mem(fut, sizeof(future(fut).value))
mem(futs...) =
    for fut in futs
        mem(
            fut,
            maximum([future(f).location.total_memory_usage for f in futs]),
        )
    end

val(fut) = loc(fut, Value(future(fut).value))

function use(f::Future)
    f
end
