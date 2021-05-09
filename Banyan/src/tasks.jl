struct BTask
    # B is for Banyan! (we can't use Task since standard library beat us to it)
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

############
# REQUESTS #
############

const Request = Union{RecordTaskRequest,RecordLocationRequest,DestroyRequest}

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

function record_request(request::Request)
    push!(get_job().pending_requests, request)
end
