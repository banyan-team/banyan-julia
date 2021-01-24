global pending_requests = Vector{Any}()

function record_request(request::Any)
	global pending_requests
	push!(pending_requests, to_jl(request))
end

function send_evaluation(value_id::ValueId, job_id::JobId)
	# TODO: Serialize requests_list to send
	global pending_requests
	#print("SENDING NOW", requests_list)
	response = send_request_get_response(
		:evaluate,
		Dict{String,Any}(
			"value_id" => value_id,
			"job_id" => job_id,
			"requests" => pending_requests
		),
	)
	empty!(pending_requests)
	return response
end

############
# REQUESTS #
############

struct RecordTaskRequest
	task::Task
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
