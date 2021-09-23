########
# Jobs #
########

# Process-local dictionary mapping from job IDs to instances of `Job`
global jobs = Dict()

# TODO: Allow for different threads to use different jobs by making this
# thread-local. For now, we only allow a single `Job` for each process
# and no sharing between threads; i.e., the burden is on the user to make
# sure they are synchronizing access to the `Job` if using the same one from
# different threads.
# TODO: Allow for different threads to use the same job by wrapping each
# `Job` in `jobs` in a mutex to allow only one to use it at a time. Further
# modifications would be required to make sharing a job between threads
# ergonomic.
global current_job_id = nothing

function set_job_id(job_id::Union{JobId, Nothing})
    global current_job_id
    current_job_id = job_id
end

function get_job_id()::JobId
    global current_job_id
    if isnothing(current_job_id)
        error("No job selected using `with_job` or `create_job` or `set_job_id`. The current job may have been destroyed or no job may have been created yet")
    end
    current_job_id
end

function get_job()
    global jobs
    jobs[get_job_id()]
end

get_cluster_name() = get_job().cluster_name

function create_job(;
    cluster_name::String = nothing,
    nworkers::Integer = 2,
    banyanfile_path::String = "",
    return_logs::Bool = false,
    store_logs_in_s3::Bool = true,
    sample_rate::Integer = nworkers,
    job_name = nothing,
    kwargs...,
)
    global jobs
    global current_job_id

    @debug "Creating job"
    if cluster_name == ""
        cluster_name = nothing
    end
    if banyanfile_path == ""
        banyanfile_path = nothing
    end

    # Configure
    configure(; kwargs...)
    cluster_name = if isnothing(cluster_name)
        clusters = list_clusters()
        if length(clusters) == 0
            error("Failed to create job: you don't have any clusters created")
        end
        first(keys(clusters))
    else
        cluster_name
    end

    # Merge Banyanfile if provided
    job_configuration = Dict{String,Any}(
        "cluster_name" => cluster_name,
        "num_workers" => nworkers,
    	"return_logs" => return_logs,
	"store_logs_in_s3" => store_logs_in_s3
    )
    if job_name != nothing
        job_configuration["job_name"] = job_name
    end
    if !isnothing(banyanfile_path)
        banyanfile = load_json(banyanfile_path)
        merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)
        for included in banyanfile["include"]
            merge_banyanfile_with!(banyanfile, included, :job, :creation)
        end
        job_configuration["banyanfile"] = banyanfile
    end

    # Create the job
    @debug "Sending request for job creation"
    job_response = send_request_get_response(:create_job, job_configuration)
    if !job_response["ready_for_jobs"]
        @debug "Updating cluster with default banyanfile"
        @info "Waiting for cluster named \"$cluster_name\" to become currently available for running a job"
        # Upload/send defaults for pt_lib.jl and pt_lib_info.json
        banyan_dir = dirname(dirname(pathof(Banyan)))
	#s3_bucket_name = s3_bucket_arn_to_name(get_cluster(name=cluster_name).s3_bucket_arn)
	#pt_lib_path = "file://$banyan_dir/res/pt_lib.jl"
	#s3_put(get_aws_config(), s3_bucket_name, basename(pt_lib_path), String(read(open(pt_lib_path[8:end]))))
	#job_configuration["pt_lib_info"] = load_json("file://$banyan_dir/res/pt_lib_info.json")
        update_cluster(
            name=cluster_name,
	    banyanfile_path="file://$(banyan_dir)/res/Banyanfile.json",
	    for_creation_or_update=:update,
#	    banyanfile=Dict(
#		"include" => [],
#	        "require" => Dict(
#		    "language" => "jl",
#		    "cluster" => Dict(
#		        "files" => [],
#			"scripts" => [],
#			"packages" => [],
#		        "pt_lib" => "file://$(banyan_dir)/res/pt_lib.jl",
#			"pt_lib_info" => "file://$(banyan_dir)/res/pt_lib_info.json"
#		    ),
#		    "job" => Dict()
#		)
#	    )
	)
	sleep(5)
	# Wait for cluster to finish updating
	while get_cluster(cluster_name).status == :updating
	   sleep(5)
	   @debug "Cluster is still updating."
	end
	# Try again
	if get_cluster(cluster_name).status != :running
	    job_response = send_request_get_response(:create_job, job_configuration)
	else
	    error("Please update the cluster with a pt_lib_info.json and pt_lib.jl")
	end
    end
    if !job_response["ready_for_jobs"]
        error("Please update the cluster with a pt_lib_info.json and pt_lib.jl")
    end
    job_id = job_response["job_id"]
    @debug "Creating job $job_id"
    @info "Created job with ID $job_id on cluster named \"$cluster_name\""

    # Store in global state
    current_job_id = job_id
    if is_debug_on()
        @show nworkers
        @show sample_rate
    end
    jobs[current_job_id] = Job(cluster_name, current_job_id, nworkers, sample_rate)
    jobs[current_job_id].current_status = "running"

    @debug "Finished creating job $job_id"
    return job_id
end

global jobs_destroyed_recently = Set()

function destroy_job(job_id::JobId; failed = nothing, force=false, kwargs...)
    global current_job_id
    global jobs_destroyed_recently
    
    if job_id in jobs_destroyed_recently && !force
        @info "Job with ID $job_id already destroyed; use force=true to destroy anyway"
        return nothing
    else
        push!(jobs_destroyed_recently, job_id)
    end

    if isnothing(failed)
        failed = false
        if !isnothing(current_job_id) && get_job().current_status == "failed"
    	    failed = true
        end
    end


    # configure(; kwargs...)

    @debug "Destroying job with ID $job_id"
    send_request_get_response(
        :destroy_job,
        Dict{String,Any}("job_id" => job_id, "failed" => failed),
    )
    @info "Destroyed job with ID $job_id"

    # Remove from global state
    if !isnothing(current_job_id) && get_job_id() == job_id
        set_job_id(nothing)
    end
    delete!(jobs, job_id)
end

function get_jobs(cluster_name=nothing; status=nothing, kwargs...)
    @debug "Downloading description of jobs in each cluster"
    configure(; kwargs...)
    filters = Dict()
    if !isnothing(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    if !isnothing(status)
        filters["status"] = status
    end
    
    # finished = false
    # Jobs = []
    # Last_eval, next_jobs = describe_jobs()
    # jobs.append(next_jobs)
    # If (last_eval = None)
    # Finished = True
    # While finished = False0
    # New_eval, next_jobs = describe_jobs(last_eval)
    # jobs.append(next_jobs)
    # if(new_eval = None)
    #         Finished = True
    # Last_eval = new_eval
    # return jobs

    
    response = Dict("last_eval_key" => 50394, "jobs" => [])
    finished = false
    indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters))
    response = indiv_response
    if indiv_response["last_eval"] == nothing 
        finished = true
    else
        curr_last_eval = indiv_response["last_eval"]
        while finished == false
            println(curr_last_eval)
            indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters, "thisStartKey"=>curr_last_eval))
            response["jobs"] = merge!(response["jobs"], indiv_response["jobs"])
            # print(indiv_response["last_eval"])
            if indiv_response["last_eval"] == nothing 
                finished = true
            else
                curr_last_eval = indiv_response["last_eval"]
            end
        end
    end
    # response =
    #     send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters))
    
    for (id, j) in response["jobs"]
        if response["jobs"][id]["ended"] == ""
	    response["jobs"][id]["ended"] = nothing
	else
	    response["jobs"][id]["ended"] = parse_time(response["jobs"][id]["ended"])
	end
	response["jobs"][id]["created"] = parse_time(response["jobs"][id]["created"])
    end
    response["jobs"]
end

function get_running_jobs(cluster_name=nothing; kwargs...)
    @debug "Downloading description of jobs in each cluster"
    configure(; kwargs...)
    filters = Dict(
	"status" => "running"
    )
    if !isnothing(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    response = 
        send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters))
    response["jobs"]
end

function download_job_logs(job_id::JobId, cluster_name::String, filename::String; kwargs...)
    @debug "Downloading logs for job"
    configure(; kwargs...)
    s3_bucket_arn = get_cluster(cluster_name).s3_bucket_arn
    s3_bucket_name = s3_bucket_arn_to_name(s3_bucket_arn)
    log_file_name = "banyan-log-for-job-$(job_id)"
    s3_get_file(get_aws_config(), s3_bucket_name, log_file_name, filename)
end

function destroy_all_jobs(cluster_name::String; kwargs...)
    @debug "Destroying all running jobs for cluster"
    configure(; kwargs...)
    jobs = get_jobs(cluster_name, status="running")
    for (job_id, job) in jobs
        if job["status"] == "running"
	    @info "Destroying job id $job_id"
            destroy_job(job_id, kwargs...)
	end
    end
end

# destroy_job() = destroy_job(get_job_id())

function with_job(f::Function; kwargs...)
    # This is not a constructor; this is just a function that ensures that
    # every job is always destroyed even in the case of an error
    use_existing_job = :job in keys(kwargs)
    destroy_job_on_error = get(kwargs, :destroy_job_on_error, true)
    destroy_job_on_exit = get(kwargs, :destroy_job_on_exit, true)
    j = use_existing_job ? kwargs[:job] : create_job(;kwargs...)
    destroyed = false
    try
        f(j)
    catch err
        # If there is an error we definitely destroy the job
        # TODO: Cache the job so that even if there is a failure we can still
        # reuse it
        if destroy_job_on_error
            destroy_job(j)
            destroyed = true
        end
        rethrow(err)
    finally
        # We only destroy the job if it hasn't already been destroyed because
        # of an error and if we don't intend to reuse a job
        if destroy_job_on_exit && !destroyed
    	    destroy_job(j)
        end
    end
end

function clear_jobs()
    global jobs
    global current_job_id
    if !isnothing(current_job_id)
        empty!(jobs[current_job_id].pending_requests)
    end
end

# TODO: Fix bug causing nbatches to be 2 when it should be 25
# TODO: Fix finalizer of Job
