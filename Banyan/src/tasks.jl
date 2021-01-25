struct BTask
    code::String
	value_names::Dict{ValueId, String}
	locations::Dict{ValueId, Location}
    effects::Dict{ValueId, String}
	pa_union::Vector{PartitionAnnotation}
	ready_to_destroy::Vector{ValueId}
end

function to_jl(task::BTask)
	return Dict(
		"code" => task.code,
		"value_names" => task.value_names,
		"locations" => Dict(id => to_jl(loc) for (id, loc) in task.locations),
		"effects" => task.effects,
		"pa_union" => [
			to_jl(pa) for pa in task.pa_union
		],
		"ready_to_destroy" => unique(task.ready_to_destroy),
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

function to_jl(record_task_request::RecordTaskRequest)
	return Dict(
		"type" => "RECORD_TASK",
		"task" => to_jl(record_task_request.task)
	)
end

# NOTE: The sole purpose of the "request" abstraction here is to potentially
# support additional kinds of requests in the future. Right now, the only thing
# we send on evaluation is the ID of the value to evaluate and tasks to record
# in a dependency graph
