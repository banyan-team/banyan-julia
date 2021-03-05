# TODO: Support multi-threaded usage by storing a global array with a job ID
# for each thread
global current_job_id = nothing

function get_job_id()::JobId
    global current_job_id
    current_job_id
end

function create_job(username::String, cluster_id::String, num_workers::Integer)::JobId
	global current_job_id

	# Create the job
	job_id = send_request_get_response(
		:create_job,
		Dict{String,Any}(
			"username" => username,
			"cluster_id" => cluster_id,
			"num_workers" => num_workers,
		),
	)["job_id"]

	println("Creating job $job_id")

	# Store in global state
	current_job_id = job_id

	return job_id
end

function destroy_job(job_id::JobId)
	global current_job_id

	@debug "Destroying job"

	send_request_get_response(
		:destroy_job,
		Dict{String,Any}("job_id" => job_id),
	)

	if current_job_id == job_id
		current_job_id = nothing
	end
end

destroy_job() = destroy_job(get_job_id())

mutable struct Job
	job_id::JobId
	username::String
	cluster_id::String
	num_workers::Integer

	function Job(username::String, cluster_id::String, num_workers::Integer)
		new_job_id = create_job(username, cluster_id, num_workers)
		new_job = new(new_job_id, username, cluster_id, num_workers)

		finalizer(new_job) do j
			destroy_job(j.job_id)
		end

		new_job
	end
end

function clear_jobs()
	global pending_requests
	empty!(pending_requests)
end

# TODO: Fix bug causing nbatches to be 2 when it should be 25
# TODO: Fix finalizer of Job
