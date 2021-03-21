# TODO: Support multi-threaded usage by storing a global array with a job ID
# for each thread
global current_job_id = nothing

function get_job_id()::JobId
    global current_job_id
    current_job_id
end

function create_job(;cluster_name::String = nothing, nworkers::Integer = 2, kwargs...)::JobId
	global current_job_id

	# Configure
	configure(;kwargs...)
	cluster_name = if isnothing(cluster_name) first(keys(list_clusters())) else cluster_name end

	# Create the job
	job_id = send_request_get_response(
		:create_job,
		Dict{String,Any}(
			"cluster_id" => cluster_name,
			"num_workers" => nworkers,
		),
	)

	print(job_id)
	job_id = job_id["job_id"]
	println("Creating job $job_id")

	# Store in global state
	current_job_id = job_id

	return job_id
end

function destroy_job(job_id::JobId; kwargs...)
	global current_job_id

	configure(;kwargs...)

	@debug "Destroying job"
	#println("destroying job ", job_id, " now?")
	send_request_get_response(
		:destroy_job,
		Dict{String,Any}("job_id" => job_id),
	)

	if current_job_id == job_id
		current_job_id = nothing
	end
end

# destroy_job() = destroy_job(get_job_id())

mutable struct Job
	job_id::JobId

	function Job(;kwargs...)
		new_job_id = create_job(;kwargs...)
		new_job = new(new_job_id)

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

function use(j::Job)
    j
end

function use(f::Future)
	f
end

# TODO: Fix bug causing nbatches to be 2 when it should be 25
# TODO: Fix finalizer of Job
