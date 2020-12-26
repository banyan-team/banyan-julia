using JSON
using Serialization

#########
# Types #
#########

mutable struct Future
    value::Any
    value_id::ValueId
    mutated::Bool
    location_type::LocationType
    function Future(job_id::String, value::Any, lt::LocationType)
        global futures

        # Generate new value id
        value_id = create_value_id()

        # Create new Future and add to futures dictionary
        new_future = new(value, value_id, false, lt)
        if lt.src_name == "Client"  # TODO: Change this name
            futures[value_id] = new_future
        end

        # Create finalizer and register
        function destroy_future(fut)
            global futures
            # TODO: Do we need to send eviction request to executor
            delete!(futures, fut.value_id)
        end
        finalizer(destroy_future, new_future)
    end
    function Future(value = nothing)
        # TODO: What should default location type be?? or init to Nothing?
        Future(get_job_id(), value, LocationType("New", "None", [], [], 1024))
    end
end


################
# Global State #
################

global futures = Dict{ValueId,Future}()

#################
# Magic Methods #
#################

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

function evaluate(job_id::String, fut::Future)
    global futures
    # Only evaluate if future has been mutated
    if true  #fut.mutated == true
        println("EVALUATE getting sent")
        fut.mutated = false

        # Send evaluate request
        response = send_evaluation(job_id, fut.value_id)

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

evaluate(fut::Future) = evaluate(get_job_id(), fut)
