struct BTask
    code::String
    value_names::Dict{ValueId,String}
    effects::Dict{ValueId,String}
    pa_union::Vector{PartitionAnnotation}
end

function to_jl(task::BTask)
    return Dict(
        "code" => task.code,
        "value_names" => task.value_names,
        "effects" => task.effects,
        "pa_union" => [to_jl(pa) for pa in task.pa_union],
    )
end

global pending_requests = Vector{Any}()

function record_request(request::Any)
    global pending_requests
    push!(pending_requests, request)
end

############
# REQUESTS #
############

struct RecordTaskRequest
    task::BTask
end

struct RecordLocationRequest
    value_id::ValueId
    location::Location
end

struct DestroyRequest
    value_id::ValueId
end

function to_jl(req::RecordTaskRequest)
    return Dict("type" => "RECORD_TASK", "task" => to_jl(req.task))
end

function to_jl(req::RecordLocationRequest)
    return Dict(
        "type" => "RECORD_LOCATION",
        "value_id" => req.value_id,
        "location" => to_jl(req.location),
    )
end

function to_jl(req::DestroyRequest)
    return Dict("type" => "DESTROY", "value_id" => req.value_id)
end

# NOTE: The sole purpose of the "request" abstraction here is to potentially
# support additional kinds of requests in the future. Right now, the only thing
# we send on evaluation is the ID of the value to evaluate and tasks to record
# in a dependency graph
