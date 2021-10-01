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

function set_job(job_id::Union{JobId,Nothing})
    global current_job_id
    current_job_id = job_id
end

function get_job_id()::JobId
    global current_job_id
    if isnothing(current_job_id)
        error(
            "No job selected using `create_job` or `with_job` or `set_job`. The current job may have been destroyed or no job may have been created yet",
        )
    end
    current_job_id
end

function get_job(job_id=get_job_id())::Job
    global jobs
    if !haskey(jobs, job_id)
        error("The selected job does not have any information; if it was created by this process, it has either failed or been destroyed.")
    end
    jobs[job_id]
end

get_cluster_name() = get_job().cluster_name

function create_job(;
    cluster_name::String = nothing,
    nworkers::Integer = 2,
    return_logs::Bool = false,
    store_logs_in_s3::Bool = true,
    sample_rate::Integer = nworkers,
    job_name = nothing,
    files = [],
    code_files = [],
    force_update_files = false,
    pf_dispatch_table = "",
    url = nothing,
    branch = nothing,
    directory = nothing,
    dev_paths = [],
    force_reclone = false,
    kwargs...,
)
    global jobs
    global current_job_id

    @debug "Creating job"
    if cluster_name == ""
        cluster_name = nothing
    end

    # TODO: Remove this code when @calebwin's PR gets merged in
    # push!(code_files, "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib.jl")
    # push!(files, "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib_utils.jl")

    # Configure
    configure(; kwargs...)

    # Construct parameters for creating job
    cluster_name = if isnothing(cluster_name)
        clusters = list_clusters()
        if length(clusters) == 0
            error("Failed to create job: you don't have any clusters created")
        end
        first(keys(clusters))
    else
        cluster_name
    end
    
    julia_version = get_julia_version()

    job_configuration = Dict{String,Any}(
        "cluster_name" => cluster_name,
        "num_workers" => nworkers,
    	"return_logs" => return_logs,
	    "store_logs_in_s3" => store_logs_in_s3,
        "julia_version" => julia_version
    )
    if !isnothing(job_name)
        job_configuration["job_name"] = job_name
    end

    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)

    environment_info = Dict{String,Any}()
    # If a url is not provided, then use the local environment
    if isnothing(url)
        # TODO: Optimize to not have to send tomls on every call
        local_environment_dir = get_julia_environment_dir()
        project_toml = load_file("file://$(local_environment_dir)Project.toml")
        if !isfile("$(local_environment_dir)Manifest.toml")
            manifest_toml = ""
            @warn "Manifest file not present for this environment"
        else
            manifest_toml = load_file("file://" * local_environment_dir * "Manifest.toml")
        end
        environment_hash = get_hash(project_toml * manifest_toml)
        environment_info["environment_hash"] = environment_hash
        environment_info["project_toml"] = "$(environment_hash)/Project.toml"
        if !isfile(S3Path("s3://$(s3_bucket_name)/$(environment_hash)/Project.toml", config=get_aws_config()))
            s3_put(get_aws_config(), s3_bucket_name, "$(environment_hash)/Project.toml", project_toml)
        end
        if manifest_toml != ""
            environment_info["manifest_toml"] = "$(environment_hash)/Manifest.toml"
            if !isfile(S3Path("s3://$(s3_bucket_name)/$(environment_hash)/Manifest.toml", config=get_aws_config()))
                s3_put(get_aws_config(), s3_bucket_name, "$(environment_hash)/Manifest.toml", manifest_toml)
            end
        end
    else
        # Otherwise, use url and optionally a particular branch
        environment_info["url"] = url
        if isnothing(directory)
            error("Directory must be provided for a url")
        end
        environment_info["directory"] = directory
        if !isnothing(branch)
            environment_info["branch"] = branch
        end
        environment_info["dev_paths"] = dev_paths
        environment_info["force_reclone"] = force_reclone
        environment_info["environment_hash"] = get_hash(
            url * directory * (if isnothing(branch) "" else branch end)
        )
    end
    job_configuration["environment_info"] = environment_info

    # Upload files to S3
    for f in vcat(files, code_files)
        s3_path = S3Path("s3://$(s3_bucket_name)/$(basename(f))", config=get_aws_config())
        if !isfile(s3_path) || force_update_files
            s3_put(get_aws_config(), s3_bucket_name, basename(f), load_file(f))
        end
    end
    # TODO: Optimize so that we only upload(and download onto cluster) the files if the filename doesn't already exist
    job_configuration["files"] = [basename(f) for f in files]
    job_configuration["code_files"] = [basename(f) for f in code_files]

    if pf_dispatch_table == ""
        pf_dispatch_table = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib_info.json"
    end
    job_configuration["pf_dispatch_table"] = load_json(pf_dispatch_table)

    if pt_lib_info == ""
        pt_lib_info = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib_info.json"
    end
    job_configuration["pt_lib_info"] = load_json(pt_lib_info)

    # Create the job
    @debug "Sending request for job creation"
    job_response = send_request_get_response(:create_job, job_configuration)
    # if !job_response["ready_for_jobs"]
    #     # Try again
    #     job_response = send_request_get_response(:create_job, job_configuration)
    # end
    # if !job_response["ready_for_jobs"]
    #     error("Please update the cluster with a pt_lib_info.json and pt_lib.jl")
    # end
    job_id = job_response["job_id"]
    @debug "Creating job $job_id"
    @info "Started creating job with ID $job_id on cluster named \"$cluster_name\""

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

function destroy_job(job_id::JobId = get_job_id(); failed = nothing, force = false, kwargs...)
    global jobs
    global current_job_id

    # TODO: Set current_status of job if failed=true

    # Set failed flag
    # failed = failed == true || !haskey(jobs, job_id) && get_job(job_id).current_status == "failed"
    # if failed && haskey(jobs, job_id)
    #     job = get_job(job_id)
    #     job.current_status = "failed"
    # end

    # configure(; kwargs...)

    @info "Destroying job with ID $job_id"
    send_request_get_response(
        :destroy_job,
        Dict{String,Any}("job_id" => job_id, "failed" => failed == true),
    )

    # Remove from global state
    if !isnothing(current_job_id) && get_job_id() == job_id
        set_job(nothing)
    end
    delete!(jobs, job_id)
end

function get_jobs(cluster_name = nothing; status = nothing, kwargs...)
    @debug "Downloading description of jobs in each cluster"
    configure(; kwargs...)
    filters = Dict()
    if !isnothing(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    if !isnothing(status)
        filters["status"] = status
    end
    
    
    response = Dict("last_eval_key" => 50394, "jobs" => [])
    finished = false
    indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters))
    response = indiv_response
    if  isnothing(indiv_response["last_eval"])
        finished = true
    else
        curr_last_eval = indiv_response["last_eval"]
        while finished == false
            if is_debug_on()
                println(curr_last_eval)
            end
            indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters, "this_start_key"=>curr_last_eval))
            response["jobs"] = merge!(response["jobs"], indiv_response["jobs"])
            # print(indiv_response["last_eval"])
            if isnothing(indiv_response["last_eval"])
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

get_running_jobs(args...; kwargs...) = get_jobs(args...; status="running", kwargs...)

function download_job_logs(job_id::JobId, cluster_name::String, filename::String=nothing; kwargs...)
    @debug "Downloading logs for job"
    configure(; kwargs...)
    s3_bucket_arn = get_cluster(cluster_name).s3_bucket_arn
    s3_bucket_name = s3_bucket_arn_to_name(s3_bucket_arn)
    log_file_name = "banyan-log-for-job-$(job_id)"
    filename = !isnothing(filename) ? filename : joinpath(homedir(), ".banyan", "logs")
    s3_get_file(get_aws_config(), s3_bucket_name, log_file_name, filename)
    @info "Downloaded logs for job with ID $job_id to $filename"
end

function destroy_all_jobs(cluster_name::String; kwargs...)
    @debug "Destroying all running jobs for cluster"
    configure(; kwargs...)
    jobs = get_jobs(cluster_name, status = "running")
    for (job_id, job) in jobs
        if job["status"] == "running"
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
    j = use_existing_job ? kwargs[:job] : create_job(; kwargs...)
    destroyed = false # because of weird catch/finally stuff
    try
        set_job(j)
        f(j)
    catch
        # If there is an error we definitely destroy the job
        # TODO: Cache the job so that even if there is a failure we can still
        # reuse it
        if destroy_job_on_error
            destroy_job(j)
            destroyed = true
        end
        rethrow()
    finally
        # We only destroy the job if it hasn't already been destroyed because
        # of an error and if we don't intend to reuse a job
        if destroy_job_on_exit && !destroyed
            destroy_job(j)
        end
    end
end

# `create_job` creates a new job. `use_job` will create the job or reuse an
# existing job created by this process with the same configuration if possible.
# `with_job`

# Every job is owned by some process that created it. Until that process
# calls destroy_job, it maintains ownership.

# create_job should be optimized to try to re-use resources if possible. If a
# job was created by the current process with the same configuration, that job
# will be reused (unless force=true).

# In an interactive use-case, re-running a notebook and creating a job should
# probably anyway create a new job since it is running again. And the way a
# notebook is used is that you only re-run the cell that you edit. So you will
# keep re-using the job. If anything, we should have an option to automatically
# destroy the job after some time or make it so that if a job is used but it is
# destroyed, then the job should be reinstated. Actually, never mind. We
# shouldn't reinstate the job. If something fails and it is a hard failure, the
# job should be restarted. The only thing we should do for that is make sure
# that recoverable exceptions on the backend are propagated while errors that
# cause the job to crash result in the job being ended. Then we should reuse
# destroyed jobs that haven't crashed.