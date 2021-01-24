global current_job_id = nothing

function get_job_id()::JobId
    global current_job_id
    current_job_id
end

function create_job(cluster_id::String, num_workers::Integer)::JobId
	global current_job_id

	# Create the job
	job_id = send_request_get_response(
		:create_job,
		Dict{String,Any}(
			"cluster_id" => cluster_id,
			"num_workers" => num_workers,
		),
	)["job_id"]

	println("Creating job $job_id")

	# Store in global state
	current_job_id = job_id

	# Run bash script to submit slurm job to create job
	# TODO: Move this functionality directly into create_job Lambda function
	ssh_key_pair =
		"SSH_KEY_PAIR" in keys(ENV) ? ENV["SSH_KEY_PAIR"] : "EC2ConnectKeyPair"
	script_path = joinpath(@__DIR__, "create_job.sh")
	run(`bash $script_path $cluster_id $ssh_key_pair $job_id $num_workers`)

	return job_id
end

function destroy_job(j)
	global current_job_id

	@debug "Destroying job"

	script_path = joinpath(@__DIR__, "destroy_job.sh")
	cluster_id = j.cluster_id
	ssh_key_pair =
		"SSH_KEY_PAIR" in keys(ENV) ? ENV["SSH_KEY_PAIR"] : "EC2ConnectKeyPair"
	job_id = j.job_id
	run(`bash $script_path $cluster_id $ssh_key_pair $job_id`)

	send_request_get_response(
		:destroy_job,
		Dict{String,Any}("job_id" => job_id),
	)

	if current_job_id == job_id
		current_job_id = nothing
	end
end

mutable struct Job
	job_id::JobId
	cluster_id::String
	num_workers::Integer

	function Job(cluster_id::String, num_workers::Integer)
		new_job_id = create_job(cluster_id, num_workers)
		new_job = new(new_job_id, cluster_id, num_workers)

		finalizer(new_job) do j
			destroy_job(j)
		end

		new_job
	end
end
