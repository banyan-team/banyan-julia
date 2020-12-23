struct JobConfig
    cluster_id::ClusterId
    num_workers::Int32
end

# TODO: Support multiple jobs and make this thread-local
global current_cluster_id = nothing
global current_job_id = nothing
global current_job_config = nothing

function set_cluster_id(cluster_id::ClusterId)
	global current_cluster_id
	current_cluster_id = cluster_id
end

function get_cluster_id()::ClusterId
	return current_cluster_id
end

function set_job_config(config::JobConfig)
    global current_job_config
    current_job_config = config
end

function get_job_id()::JobId
    global current_job_id
    global current_job_config
    if isnothing(current_job_id) &&
       !isnothing(current_job_config)
        create_job(current_job_config, make_current = true)
    end
    return current_job_id
end

job_exists() = !isnothing(current_job_id)


function create_job(config::JobConfig; make_current = false,)
	global current_job_id

	# Create the job
	job_id = send_request_get_response(
		:create_job,
		Dict{String,Any}(
			"language" => "jl",
			"cluster_id" => config.cluster_id,
			"num_workers" => config.num_workers,
		),
	)["job_id"]

	@debug "Creating job $job_id"

	# If making current, store in global state
	if make_current
		current_job_id = job_id
	end

	# Run bash script to submit slurm job to create job
	# TODO: Move this functionality directly into create_job Lambda function
	ssh_key_pair =
		"SSH_KEY_PAIR" in keys(ENV) ? ENV["SSH_KEY_PAIR"] : "EC2ConnectKeyPair"
	cluster_id = config.cluster_id
	script_path = joinpath(@__DIR__, "create_job.sh")
	run(`bash $script_path $cluster_id $ssh_key_pair $job_id $num_workers`)
	return job_id
end

function destroy_job(cluster_id::ClusterId, job_id::JobId)
	global current_job_id

	@debug "Destroying job"

	ssh_key_pair =
		"SSH_KEY_PAIR" in keys(ENV) ? ENV["SSH_KEY_PAIR"] : "EC2ConnectKeyPair"
	script_path = joinpath(@__DIR__, "destroy_job.sh")
	run(`bash $script_path $cluster_id $ssh_key_pair $job_id`)

	send_request_get_response(
		:destroy_job,
		Dict{String,Any}("job_id" => job_id),
	)
	if current_job_id == job_id
		current_job_id = nothing
	end
end

function destroy_job()
	destroy_job(get_cluster_id(), get_job_id())
end