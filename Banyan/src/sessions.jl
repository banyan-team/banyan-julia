# Functions for starting and ending sessions


function start_session(;
    cluster_name::Union{String,Nothing} = nothing,
    nworkers::Union{Integer,Nothing} = 16,
    resource_release_delay::Integer = 20,
    print_logs::Union{Bool,Nothing} = false,
    store_logs_in_s3::Union{Bool,Nothing} = true,
    store_logs_on_cluster::Union{Bool,Nothing} = false,
    sample_rate::Union{Integer,Nothing} = nworkers,
    session_name::Union{String,Nothing} = nothing,
    files::Union{Vector,Nothing} = [],
    code_files::Union{Vector,Nothing} = [],
    force_update_files::Union{Bool,Nothing} = false,
    pf_dispatch_table::Union{String,Nothing} = "",
    using_modules::Union{Vector,Nothing} = [],
    url::Union{String,Nothing} = nothing,
    branch::Union{String,Nothing} = nothing,
    directory::Union{String,Nothing} = nothing,
    dev_paths::Union{Vector,Nothing} = [],
    force_clone::Union{Bool,Nothing} = false,
    force_pull::Union{Bool,Nothing} = false,
    force_install::Union{Bool,Nothing} = false,
    force_restart::Union{Bool,Nothing} = false,
    nowait::Bool=false,
    email_address::String=nothing,
    kwargs...,
)::JobId  # TODO: This should return a session ID

    global jobs
    global current_job_id

    # Configure
    configure(; kwargs...)

    # Construct parameters for starting session
    cluster_name = if isnothing(cluster_name)
        running_clusters = get_running_clusters()
        if length(running_clusters) == 0
            error("Failed to start session: you don't have any clusters created")
        end
        first(keys(running_clusters))
    else
        cluster_name
    end

    julia_version = get_julia_version()

    session_configuration = Dict{String,Any}(
        "cluster_name" => cluster_name,
        "num_workers" => nworkers,
        "resource_release_delay" => resource_release_delay,
        "return_logs" => print_logs,
        "store_logs_in_s3" => store_logs_in_s3,
        "store_logs_on_cluster" => store_logs_on_cluster,
        "julia_version" => julia_version,
        "benchmark" => get(ENV, "BANYAN_BENCHMARK", "0") == "1",
        "main_modules" => get_loaded_packages(),
        "using_modules" => using_modules,
        "force_restart" => force_restart,
        "email_address" => email_address,
    )
    if !isnothing(session_name)
        session_configuration["session_name"] = session_name
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
            error("Directory must be provided for given URL $url")
        end
        environment_info["directory"] = directory
        if !isnothing(branch)
            environment_info["branch"] = branch
        end
        environment_info["dev_paths"] = dev_paths
        environment_info["force_clone"] = force_clone
        environment_info["force_pull"] = force_pull
        environment_info["force_install"] = force_install
        environment_info["environment_hash"] = get_hash(
            url * (if isnothing(branch) "" else branch end)
        )
    end
    session_configuration["environment_info"] = environment_info

    # Upload files to S3
    for f in vcat(files, code_files)
        s3_path = S3Path("s3://$(s3_bucket_name)/$(basename(f))", config=get_aws_config())
        if !isfile(s3_path) || force_update_files
            s3_put(get_aws_config(), s3_bucket_name, basename(f), load_file(f))
        end
    end
    # TODO: Optimize so that we only upload (and download onto cluster) the files if the filename doesn't already exist
    session_configuration["files"] = [basename(f) for f in files]
    session_configuration["code_files"] = [basename(f) for f in code_files]

    if pf_dispatch_table == ""
        pf_dispatch_table = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pf_dispatch_table.json"
    end
    session_configuration["pf_dispatch_table"] = load_json(pf_dispatch_table)

    # Start the session
    @debug "Sending request for session start"
    response = send_request_get_response(:start_session, session_configuration)
    job_id = response["job_id"]
    @info "Starting session with ID $job_id on cluster named \"$cluster_name\""

    # Store in global state
    current_job_id = job_id
    jobs[current_job_id] = Job(cluster_name, current_job_id, nworkers, sample_rate)

    wait_for_cluster(cluster_name)

    if !nowait
        wait_for_session(job_id)
    end

    @debug "Finished starting session $job_id"
    job_id
end

function end_session(job_id::JobId = get_job_id(); failed = false, force = false, resource_release_delay = nothing, kwargs...)
    global jobs
    global current_job_id

    @info "Ending session with ID $job_id"
    request_params = Dict{String,Any}("job_id" => job_id, "failed" => failed)
    if !isnothing(resource_release_delay)
        request_params["resource_release_delay"] = resource_release_delay
    end
    send_request_get_response(
        :end_session,
        request_params,
    )

    # Remove from global state
    set_job(nothing)
    delete!(jobs, job_id)
    job_id
end

function get_sessions(cluster_name = nothing; status = nothing, kwargs...)
    @debug "Downloading description of all sessions in cluster named $cluster_name"
    configure(; kwargs...)
    filters = Dict()
    if !isnothing(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    if !isnothing(status)
        filters["status"] = status
    end

    finished = false
    indiv_response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters))
    response = indiv_response
    if  isnothing(indiv_response["last_eval"])
        finished = true
    else
        curr_last_eval = indiv_response["last_eval"]
        while !finished
            indiv_response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters, "this_start_key"=>curr_last_eval))
            response["sessions"] = merge!(response["sessions"], indiv_response["sessions"])
            if isnothing(indiv_response["last_eval"])
                finished = true
            else
                curr_last_eval = indiv_response["last_eval"]
            end
        end
    end
    
    for (id, j) in response["sessions"]
        if response["sessions"][id]["ended"] == ""
            response["sessions"][id]["ended"] = nothing
        else
            response["sessions"][id]["ended"] = parse_time(response["sessions"][id]["ended"])
        end
        response["sessions"][id]["created"] = parse_time(response["sessions"][id]["created"])
    end
    response["sessions"]
end

get_running_sessions(args...; kwargs...) = get_sessions(args...; status="running", kwargs...)

function download_session_logs(job_id::JobId, cluster_name::String, filename::String=nothing; kwargs...)
    @debug "Downloading logs for session"
    configure(; kwargs...)
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    log_file_name = "banyan-log-for-job-$(job_id)"
    filename = !isnothing(filename) ? filename : joinpath(homedir(), ".banyan", "logs")
    s3_get_file(get_aws_config(), s3_bucket_name, log_file_name, filename)
    @info "Downloaded logs for session with ID $job_id to $filename"
end

function end_all_sessions(cluster_name::String; kwargs...)
    @info "Ending all running sessions for cluster named $cluster_name"
    configure(; kwargs...)
    sessions = get_sessions(cluster_name, status=["creating", "running"])
    for (job_id, session) in sessions
        if session["status"] == "running"
            end_session(job_id, kwargs...)
        end
    end
end

function get_session_status(job_id::String=get_job_id(); kwargs...)
    configure(; kwargs...)
    filters = Dict("job_id" => job_id)
    response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters))
    session_status = response["sessions"][job_id]["status"]
    if session_status == "failed"
        # We don't immediately fail - we're just explaining. It's only later on
        # where it's like we're actually using this job do we set the status.
        @error response["sessions"][job_id]["status_explanation"]
    end
    session_status
end

function wait_for_session(job_id::JobId=get_job_id(), kwargs...)
    t = 5
    session_status = get_session_status(job_id; kwargs)
    p = ProgressUnknown("Preparing session with ID $job_id", spinner=true)
    while session_status == "creating"
        sleep(t)
        next!(p)
        if t < 80
            t *= 2
        end
        session_status = get_session_status(job_id; kwargs)
    end
    finish!(p, spinner = session_status == "running" ? '✓' : '✗')
    if session_status == "running"
        @debug "Session with ID $job_id is ready"
    elseif session_status == "completed"
        error("Session with ID $job_id has already completed")
    elseif session_status == "failed"
        error("Session with ID $job_id has failed")
    else
        error("Unknown session status $session_status")
    end
end

function with_session(f::Function; kwargs...)
    # This is not a constructor; this is just a function that ensures that
    # every session is always destroyed even in the case of an error
    use_existing_session = :session in keys(kwargs)
    end_session_on_error = get(kwargs, :end_session_on_error, true)
    end_session_on_exit = get(kwargs, :end_session_on_exit, true)
    j = use_existing_session ? kwargs[:session] : start_session(; kwargs...)
    destroyed = false # because of weird catch/finally stuff
    try
        set_job(j)
        f(j)
    catch
        # If there is an error we definitely destroy the job
        # TODO: Cache the job so that even if there is a failure we can still
        # reuse it
        if end_session_on_error
            end_session(j)
            destroyed = true
        end
        rethrow()
    finally
        # We only destroy the job if it hasn't already been destroyed because
        # of an error and if we don't intend to reuse a job
        if end_session_on_exit && !destroyed
            end_session(j)
        end
    end
end