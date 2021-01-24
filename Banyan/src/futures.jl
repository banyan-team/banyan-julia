using JSON
using Serialization

#########
# Types #
#########

mutable struct Future
    value::Any
    value_id::ValueId
    mutated::Bool
    location::Location
    
    function Future(value = nothing, loc=None())
        global futures

        # Generate new value id
        value_id = create_value_id()

        # Create new Future and add to futures dictionary if lt is Client
        new_future = new(value, value_id, false, loc)
        if loc.src_name == "Client" || loc.dst_name == "Client"
            futures[value_id] = new_future
        end

        # Update location
        global locations
        locations[value_id] = loc

        # Create finalizer and register
        finalizer(new_future) do fut
            global futures
            # TODO: Include value ID in a global queue (like locations) of
            # value IDs to be destroyed on backend
            delete!(futures, fut.value_id)
        end

        new_future
    end
end

################
# Global State #
################

# Futures that have location as "Client"
global futures = Dict{ValueId,Future}()

# Queue of pending updates to values' locations for sending to backend
global locations = Dict{ValueId,Location}()

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

function record_mut(value_id::ValueId)
    global futures
    futures[value_id].mutated = true
end

function evaluate(fut::Future, job_id::JobId)
    println("IN EVALUATE")
    global futures

    # TODO: Only evaluate if future has been mutated
    if true  #fut.mutated == true
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
                serialize(buf, futures[value_id].value)
                value = take!(buf)
                send_message(
                    scatter_queue,
                    JSON.json(Dict{String,Any}("value_id" => value_id, "value" => value))
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
                futures[value_id].mutated = false
            elseif message_type == "EVALUATION_END"
                break
            end
        end
    end

    return getfield(fut, :value)
end

evaluate(fut::Future, job::Job) = evaluate(fut, job.job_id)

evaluate(fut::Future) = evaluate(fut, get_job_id())
