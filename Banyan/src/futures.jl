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

    function Future(value = nothing, loc = None())
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

function get_locations()
    global locations
    locations_copy = deepcopy(locations)
    empty!(locations)
    return locations_copy
end

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
                    JSON.json(Dict{String,Any}("value_id" => value_id, "value" => value)),
                )
            elseif message_type == "GATHER"
                # Receive gather
                value_id = message["value_id"]
                value = deserialize(IOBuffer(convert(Array{UInt8}, message["value"])))
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

evaluate(fut::Future, job::Job) = evaluate(fut, job.job_id)

evaluate(fut::Future) = evaluate(fut, get_job_id())

function send_evaluation(value_id::ValueId, job_id::JobId)
	# TODO: Serialize requests_list to send
	global pending_requests
	#print("SENDING NOW", requests_list)
	response = send_request_get_response(
		:evaluate,
		Dict{String,Any}(
			"value_id" => value_id,
			"job_id" => job_id,
			"requests" => [to_jl(req) for req in pending_requests]
		),
	)
	empty!(pending_requests)
	return response
end

################################
# Methods for Setting Location #
################################

function src(fut::Future, loc::Location)
    global locations
    locations[fut.value_id].src_name = loc.src_name
    locations[fut.value_id].src_parameters = loc.src_parameters
end

function dst(fut::Future, loc::Location)
    global locations
    locations[fut.value_id].dst_name = loc.dst_name
    locations[fut.value_id].dst_parameters = loc.dst_parameters
end

function loc(fut::Future, loc::Location)
    global locations
    locations[fut.value_id] = loc
end