global requests_list = Vector{Any}()

function record_request(request::Any)
	global requests_list
	push!(requests_list, to_jl(request))
end

function send_evaluation(job_id::JobId, value_id::ValueId)
	# TODO: Serialize requests_list to send
	global requests_list
	response = send_request_get_response(
		:evaluate,
		Dict{String,Any}(
			"job_id" => job_id,
			"value_id" => value_id,
			"requests_list" => requests_list
		),
	)
	empty!(requests_list)
	return response
end