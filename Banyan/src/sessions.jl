@nospecialize

# Functions for managing sessions

# Process-local dictionary mapping from session IDs to instances of `Session`
global sessions = Dict{SessionId,Session}()

# TODO: Allow for different threads to use different session by making this
# thread-local. For now, we only allow a single `Session` for each process
# and no sharing between threads; i.e., the burden is on the user to make
# sure they are synchronizing access to the `Session` if using the same one from
# different threads.
# TODO: Allow for different threads to use the same session by wrapping each
# `Session` in `sessions` in a mutex to allow only one to use it at a time. Further
# modifications would be required to make sharing a session between threads
# ergonomic.
global current_session_id = ""

function set_session(session_id::SessionId)
    global current_session_id
    current_session_id = session_id
end

function _get_session_id_no_error()::SessionId
    global current_session_id
    current_session_id
end

function get_session_id()::SessionId
    global current_session_id
    if isempty(current_session_id)
        error(
            "No session started or selected using `start_session` or `with_session` or `set_session`. The current session may have been destroyed or no session started yet.",
        )
    end
    current_session_id
end

function get_sessions_dict()::Dict{SessionId,Session}
    global sessions
    sessions
end

function get_session()::Session
    session_id = get_session_id()
    sessions_dict = get_sessions_dict()
    if !haskey(sessions_dict, session_id)
        error("The selected session does not have any information; if it was started by this process, it has either failed or been destroyed.")
    end
    sessions_dict[session_id]
end

get_cluster_name()::String = get_session().cluster_name

function get_loaded_packages()
    global current_session_id
    loaded_packages::Set{String} = if !isempty(current_session_id)
        get_sessions_dict()[current_session_id].loaded_packages
    else
        Set{String}()
    end
    res::Vector{String} = Base.collect(loaded_packages)
    for m in names(Main, imported=true)
        m_string = string(m)
        if !(m_string in loaded_packages)
            is_used = try
                Main.eval(m)
            catch
                nothing
            end
            if is_used isa Module && !(m in [:Main, :Base, :Core, :InteractiveUtils, :IJulia, :VSCodeServer])
                push!(res, m_string)
            end
        end
    end
    res
end

const NOTHING_STRING = "NOTHING_STRING"

function _start_session(
    cluster_name::String,
    nworkers::Int64,
    release_resources_after::Integer,
    print_logs::Bool,
    store_logs_in_s3::Bool,
    store_logs_on_cluster::Bool,
    log_initialization::Bool,
    sample_rate::Int64,
    session_name::String,
    files::Vector{String},
    code_files::Vector{String},
    force_update_files::Bool,
    pf_dispatch_table::Vector{String},
    no_pf_dispatch_table::Bool,
    using_modules::Vector{String},
    # We currently can't use modules that require GUI
    not_using_modules::Vector{String},
    url::String,
    branch::String,
    directory::String,
    dev_paths::Vector{String},
    force_sync::Bool,
    force_pull::Bool,
    force_install::Bool,
    estimate_available_memory::Bool,
    nowait::Bool,
    email_when_ready::Bool,
    no_email::Bool,
    for_running::Bool,
    sessions::Dict{String,Session},
)
    # Construct parameters for starting session
    cluster_name = if cluster_name == NOTHING_STRING
        running_clusters = get_running_clusters()
        if length(running_clusters) == 0
            error("Failed to start session: you don't have any clusters created")
        end
        first(keys(running_clusters))
    else
        cluster_name
    end

    version = get_julia_version()

    not_in_modules = m -> !(m in not_using_modules)
    main_modules = filter(not_in_modules, get_loaded_packages())
    using_modules = filter(not_in_modules, using_modules)
    session_configuration = Dict{String,Any}(
        "cluster_name" => cluster_name,
        "num_workers" => nworkers,
        "sample_rate" => sample_rate,
        "release_resources_after" => release_resources_after == -1 ? nothing : release_resources_after,
        "return_logs" => print_logs,
        "store_logs_in_s3" => store_logs_in_s3,
        "store_logs_on_cluster" => store_logs_on_cluster,
        "log_initialization" => log_initialization,
        "version" => version,
        "benchmark" => get(ENV, "BANYAN_BENCHMARK", "0")::String == "1",
        "main_modules" => main_modules,
        "using_modules" => using_modules,
        "reuse_resources" => !force_update_files,
        "estimate_available_memory" => estimate_available_memory,
        "language" => "jl"
    )
    if session_name != NOTHING_STRING
        session_configuration["session_name"] = session_name
    end
    if !no_email
        session_configuration["email_when_ready"] = email_when_ready
    end
    c::Cluster = get_cluster(cluster_name)
    s3_bucket_name = s3_bucket_arn_to_name(c.s3_bucket_arn)
    organization_id = c.organization_id
    curr_cluster_instance_id = c.curr_cluster_instance_id
    
    session_configuration["organization_id"] = organization_id
    session_configuration["curr_cluster_instance_id"] = curr_cluster_instance_id

    environment_info = Dict{String,Any}()
    # If a url is not provided, then use the local environment
    if url == NOTHING_STRING
        
        # TODO: Optimize to not have to send tomls on every call
        local_environment_dir = get_julia_environment_dir()
        project_toml = load_file("file://$(local_environment_dir)Project.toml")
        if !isfile("$(local_environment_dir)Manifest.toml")
            manifest_toml = ""
            @warn "Creating a session with a Julia environment that does not have a Manifest.toml"
        else
            manifest_toml = load_file("file://" * local_environment_dir * "Manifest.toml")
        end
        environment_hash = get_hash(project_toml * manifest_toml * version)
        environment_info["environment_hash"] = environment_hash
        environment_info["project_toml"] = "$(environment_hash)/Project.toml"
        file_already_in_s3 = isfile(S3Path("s3://$(s3_bucket_name)/$(environment_hash)/Project.toml", config=get_aws_config()))
        if !file_already_in_s3
            s3_put(get_aws_config(), s3_bucket_name, "$(environment_hash)/Project.toml", project_toml)
        end
        if manifest_toml != ""
            environment_info["manifest_toml"] = "$(environment_hash)/Manifest.toml"
            file_already_in_s3 = isfile(S3Path("s3://$(s3_bucket_name)/$(environment_hash)/Manifest.toml", config=get_aws_config()))
            if !file_already_in_s3
                s3_put(get_aws_config(), s3_bucket_name, "$(environment_hash)/Manifest.toml", manifest_toml)
            end
        end
    else
        # Otherwise, use url and optionally a particular branch
        environment_info["url"] = url
        if directory == NOTHING_STRING
            error("Directory must be provided for given URL $url")
        end
        environment_info["directory"] = directory
        if branch != NOTHING_STRING
            environment_info["branch"] = branch
        end
        environment_info["dev_paths"] = dev_paths
        environment_info["force_pull"] = force_pull
        environment_info["force_install"] = force_install
        environment_info["environment_hash"] = get_hash(
            url * (if branch == NOTHING_STRING "" else branch end) * join(dev_paths)
        )
    end
    environment_info["force_sync"] = force_sync
    session_configuration["environment_info"] = environment_info

    # Upload files to S3
    for f in vcat(files, code_files)
        s3_path = S3Path("s3://$(s3_bucket_name)/$(basename(f))", config=get_aws_config())
        if !isfile(s3_path) || force_update_files
            s3_put(get_aws_config(), s3_bucket_name, basename(f), load_file(f))
        end
    end
    # TODO: Optimize so that we only upload (and download onto cluster) the files if the filename doesn't already exist
    session_configuration["files"] = map(basename, files)
    session_configuration["code_files"] = map(basename, code_files)

    if no_pf_dispatch_table
        branch_to_use::String = get(ENV, "BANYAN_TESTING", "0")::String == "1" ? get_branch_name() : BANYAN_JULIA_BRANCH_NAME
        pf_dispatch_table = String[]
        for dir in BANYAN_JULIA_PACKAGES
            push!(pf_dispatch_table, "https://raw.githubusercontent.com/banyan-team/banyan-julia/$branch_to_use/$dir/res/pf_dispatch_table.toml")
        end
    end
    pf_dispatch_table_loaded = load_toml(pf_dispatch_table)
    session_configuration["pf_dispatch_table"] = pf_dispatch_table_loaded

    # Start the session
    @debug "Sending request for start_session"
    response = send_request_get_response(:start_session, session_configuration)
    session_id::SessionId = response["session_id"]
    resource_id::ResourceId = response["resource_id"]
    organization_id::String = response["organization_id"]
    cluster_instance_id::String = response["cluster_instance_id"]
    cluster_name::String = response["cluster_name"]
    reusing_resources::Bool = response["reusing_resources"]
    cluster_potentially_not_ready = response["stale_cluster_status"] != "running"
    scatter_queue_url = response["scatter_queue_url"]::String
    gather_queue_url = response["gather_queue_url"]::String
    execution_queue_url = response["execution_queue_url"]::String
    num_sessions = response["num_sessions"]::Int64
    num_workers_in_use = response["num_workers_in_use"]::Int64
    msg = begin
        message = if for_running
            "Running session with ID $session_id and $code_files"
        else
            "Starting session with ID $session_id on cluster named \"$cluster_name\""
        end
        if num_sessions == 0
            message *= " with no sessions running yet"
        elseif num_sessions == 1
            message *= " with 1 session already running"
        else
            message *= " with $num_sessions sessions already running"
        end
        if num_workers_in_use > 0
            if num_sessions == 0 
                message *= " but $num_workers_in_use workers running"
            else
                message *= " on $num_workers_in_use workers"
            end
        end
        message
    end
    @info msg
    # Store in global state
    sessions[session_id] = Session(
        cluster_name,
        session_id,
        resource_id,
        nworkers,
        sample_rate,
        organization_id,
        cluster_instance_id,
        not_using_modules,
        !cluster_potentially_not_ready,
        false;
        scatter_queue_url=scatter_queue_url,
        gather_queue_url=gather_queue_url,
        execution_queue_url=execution_queue_url
    )

    if !nowait
        wait_for_session(session_id)
    elseif !reusing_resources
        @warn "Starting this session requires creating new cloud computing resources which will take 10-30 minutes for the first computation."
    end

    @debug "Finished call to start_session with ID $session_id"
    session_id
end

function start_session(;
    cluster_name::String = NOTHING_STRING,
    nworkers::Int64 = 16,
    release_resources_after::Union{Integer,Nothing} = 20,
    print_logs::Bool = false,
    store_logs_in_s3::Bool = true,
    store_logs_on_cluster::Bool = false,
    log_initialization::Bool = false,
    sample_rate::Int64 = nworkers,
    session_name::String = NOTHING_STRING,
    files::Vector{String} = String[],
    code_files::Vector{String} = String[],
    force_update_files::Bool = false,
    pf_dispatch_table::Union{Vector{String},Nothing} = nothing,
    using_modules::Vector{String} = String[],
    # We currently can't use modules that require GUI
    not_using_modules::Vector{String} = NOT_USING_MODULES,
    url::String = NOTHING_STRING,
    branch::String = NOTHING_STRING,
    directory::String = NOTHING_STRING,
    dev_paths::Vector{String} = String[],
    force_sync::Bool = false,
    force_pull::Bool = false,
    force_install::Bool = false,
    estimate_available_memory::Bool = true,
    nowait::Bool = true,
    email_when_ready::Union{Bool,Nothing} = nothing,
    for_running::Bool = false,
    kwargs...,
)::SessionId
    # Should save 5ms of overhead
    @nospecialize

    global BANYAN_JULIA_BRANCH_NAME
    global BANYAN_JULIA_PACKAGES

    sessions = get_sessions_dict()
    global current_session_id

    # Configure
    configure(; kwargs...)
    
    current_session_id = _start_session(
        cluster_name,
        nworkers,
        isnothing(release_resources_after) ? -1 : release_resources_after,
        print_logs,
        store_logs_in_s3,
        store_logs_on_cluster,
        log_initialization,
        sample_rate,
        session_name,
        files,
        code_files,
        force_update_files,
        isnothing(pf_dispatch_table) ? String[] : pf_dispatch_table,
        isnothing(pf_dispatch_table),
        using_modules,
        # We currently can't use modules that require GUI
        not_using_modules,
        url,
        branch,
        directory,
        dev_paths,
        force_sync,
        force_pull,
        force_install,
        estimate_available_memory,
        nowait,
        isnothing(email_when_ready) ? false : email_when_ready,
        isnothing(email_when_ready),
        for_running,
        sessions
    )
    current_session_id
end

function end_session(session_id::SessionId = get_session_id(); failed = false, release_resources_now = false, release_resources_after = nothing, kwargs...)
    sessions = get_sessions_dict()
    global current_session_id

    # Configure using parameters
    configure(; kwargs...)

    request_params = Dict{String,Any}("session_id" => session_id, "failed" => failed, "release_resources_now" => release_resources_now)
    if !isnothing(release_resources_after)
        request_params["release_resources_after"] = release_resources_after
    end
    send_request_get_response(
        :end_session,
        request_params,
    )
    @info "Ending session with ID $session_id"

    # Remove from global state
    set_session("")
    delete!(sessions, session_id)
    session_id
end

function get_sessions(cluster_name = nothing; status = nothing, limit = -1, kwargs...)
    if isnothing(cluster_name)
        @debug "Downloading description of all sessions"
    else
        @debug "Downloading description of all sessions in cluster named $cluster_name"
    end
    configure(; kwargs...)
    filters = Dict()
    if !isnothing(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    if !isnothing(status)
        filters["status"] = status
    end

    if limit > 0
        # Get the last `limit` number of sessions
        indiv_response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters, "limit"=>limit))
        sessions = indiv_response["sessions"]
    else
        # Get all sessions
        indiv_response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters))
        curr_last_eval = indiv_response["last_eval"]
        sessions = indiv_response["sessions"]
        while !isnothing(curr_last_eval)
            indiv_response = send_request_get_response(:describe_sessions, Dict{String,Any}("filters"=>filters, "this_start_key"=>curr_last_eval))
            sessions = merge!(sessions, indiv_response["sessions"])
            curr_last_eval = indiv_response["last_eval"]
        end
    end
    
    for (id, j) in sessions
        if sessions[id]["end_time"] == ""
            sessions[id]["end_time"] = nothing
        else
            sessions[id]["end_time"] = parse_time(sessions[id]["end_time"])
        end
        sessions[id]["start_time"] = parse_time(sessions[id]["start_time"])
    end
    sessions
end

# TODO: Make get_resources, get_running_resources, destroy_resource
# and then make end_all_sessions call these functions to end all running jobs
# if release_resources_now=true.
# function get_resources(cluster_name = nothing; status = nothing, kwargs...)
#     @debug "Downloading description of all jobs in cluster named $cluster_name"
#     configure(; kwargs...)
#     filters = Dict()
#     if !isnothing(cluster_name)
#         filters["cluster_name"] = cluster_name
#     end
#     if !isnothing(status)
#         filters["status"] = status
#     end

#     finished = false
#     indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters))
#     response = indiv_response
#     if  isnothing(indiv_response["last_eval"])
#         finished = true
#     else
#         curr_last_eval = indiv_response["last_eval"]
#         while !finished
#             indiv_response = send_request_get_response(:describe_jobs, Dict{String,Any}("filters"=>filters, "this_start_key"=>curr_last_eval))
#             response["jobs"] = merge!(response["jobs"], indiv_response["jobs"])
#             if isnothing(indiv_response["last_eval"])
#                 finished = true
#             else
#                 curr_last_eval = indiv_response["last_eval"]
#             end
#         end
#     end
    
#     for (id, j) in response["jobs"]
#         if response["jobs"][id]["ended"] == ""
#             response["jobs"][id]["ended"] = nothing
#         else
#             response["jobs"][id]["ended"] = parse_time(response["sessions"][id]["ended"])
#         end
#         response["jobs"][id]["created"] = parse_time(response["sessions"][id]["created"])
#     end
#     response["sessions"]
# end

get_running_sessions(args...; kwargs...) = get_sessions(args...; status="running", kwargs...)

function download_session_logs(session_id::SessionId, cluster_name::String, filename::Union{String,Nothing}=nothing; kwargs...)
    @debug "Downloading logs for session"
    configure(; kwargs...)
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name; kwargs...)
    log_file_name = "banyan-log-for-session-$(session_id)"
    if isnothing(filename) & !isdir(joinpath(homedir(), ".banyan", "logs"))
        mkdir(joinpath(homedir(), ".banyan", "logs"))
    end
    filename = !isnothing(filename) ? filename : joinpath(homedir(), ".banyan", "logs", log_file_name)
    s3_get_file(get_aws_config(), s3_bucket_name, log_file_name, filename)
    @info "Downloaded logs for session with ID $session_id to $filename"
    return filename
end

function print_session_logs(session_id, cluster_name, delete_file=true)
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name; kwargs...)
    log_file_name = "banyan-log-for-session-$(session_id)"
    logs = s3_get(get_aws_config(), s3_bucket_name, log_file_name)
    println(logs)
    if delete_file
        s3_delete(get_aws_config(), s3_bucket_name, log_file_name)
    end
end

function end_all_sessions(cluster_name::String; release_resources_now = false, release_resources_after = nothing, kwargs...)
    @info "Ending all running sessions for cluster named $cluster_name"
    configure(; kwargs...)
    sessions = get_sessions(cluster_name, status=["creating", "running"])
    for (session_id, session) in sessions
        end_session(session_id; release_resources_now=release_resources_now, release_resources_after=release_resources_after, kwargs...)
    end
end

function get_session_status(session_id::String=get_session_id(); kwargs...)::String
    sessions = get_sessions_dict()
    configure(; kwargs...)
    filters = Dict{String,Any}("session_id" => session_id)
    params = Dict{String,Any}("filters"=>filters)
    if haskey(sessions, session_id)
        params["organization_id"] = sessions[session_id].organization_id
    end
    response = send_request_get_response(:describe_sessions, params)
    if !haskey(response["sessions"], session_id)
        @warn "Session with ID $session_id is assumed to still be creating"
        return "creating"
    end
    session_status = response["sessions"][session_id]["status"]
    resource_id = response["sessions"][session_id]["resource_id"]
    if haskey(sessions, session_id)
        sessions[session_id].resource_id = resource_id
    end
    if session_status == "failed"
        # We don't immediately fail - we're just explaining. It's only later on
        # where it's like we're actually using this session do we set the status.
        @error response["sessions"][session_id]["status_explanation"]
    end
    session_status
end

function _wait_for_session(session_id::SessionId=get_session_id(), kwargs...)
    sessions_dict = get_sessions_dict()
    session_status = get_session_status(session_id; kwargs...)
    p = ProgressUnknown("Preparing session with ID $session_id", spinner=true)
    t = 0
    st = time()
    while session_status == "creating"
        sleep(t)
        t = if time() - st < 90
            0
        else
            7
        end
        next!(p)
        session_status = get_session_status(session_id; kwargs...)
    end
    finish!(p, spinner = session_status == "running" ? '✓' : '✗')
    if session_status == "running"
        @debug "Session with ID $session_id is ready"
        if haskey(sessions_dict, session_id)
            sessions_dict[session_id].is_session_ready = true
        end
    elseif session_status == "completed"
        error("Session with ID $session_id has already completed")
    elseif session_status == "failed"
        error("Session with ID $session_id has failed.")
    else
        error("Unknown session status $session_status")
    end
end

function wait_for_session(session_id::SessionId=get_session_id(), kwargs...)
    sessions_dict = get_sessions_dict()
    is_session_ready = if haskey(sessions_dict, session_id)
        session_info::Session = sessions_dict[session_id]
        if !session_info.is_cluster_ready
            wait_for_cluster(session_info.cluster_name, kwargs...)
        end
        session_info.is_session_ready
    else
        false
    end
    if !is_session_ready
        _wait_for_session(session_id, kwargs...)
    end
end

function with_session(f::Function; kwargs...)
    # This is not a constructor; this is just a function that ensures that
    # every session is always destroyed even in the case of an error
    use_existing_session = :session in keys(kwargs)
    end_session_on_error = get(kwargs, :end_session_on_error, true)::Bool
    end_session_on_exit = get(kwargs, :end_session_on_exit, true)::Bool
    j = use_existing_session ? kwargs[:session] : start_session(; kwargs...)
    destroyed = false # because of weird catch/finally stuff
    try
        set_session(j)
        f(j)
    catch
        # If there is an error we definitely destroy the session
        # TODO: Cache the session so that even if there is a failure we can still
        # reuse it
        if end_session_on_error
            end_session(j)
            destroyed = true
        end
        rethrow()
    finally
        # We only end the session if it hasn't already been end because
        # of an error and if we don't intend to reuse a session
        if end_session_on_exit && !destroyed
            end_session(j)
        end
    end
end


function run_session(;
    cluster_name::Union{String,Nothing} = nothing,
    nworkers::Union{Integer,Nothing} = 16,
    release_resources_after::Union{Integer,Nothing} = 20,
    print_logs::Union{Bool,Nothing} = false,
    store_logs_in_s3::Union{Bool,Nothing} = true,
    store_logs_on_cluster::Union{Bool,Nothing} = false,
    sample_rate::Union{Integer,Nothing} = nworkers,
    session_name::Union{String,Nothing} = nothing,
    files::Union{Vector,Nothing} = [],
    code_files::Union{Vector,Nothing} = [],
    force_update_files = true,
    pf_dispatch_table::Union{String,Nothing} = nothing,
    using_modules::Union{Vector,Nothing} = [],
    url::Union{String,Nothing} = nothing,
    branch::Union{String,Nothing} = nothing,
    directory::Union{String,Nothing} = nothing,
    dev_paths::Union{Vector,Nothing} = [],
    force_sync::Union{Bool,Nothing} = false,
    force_pull::Union{Bool,Nothing} = false,
    force_install::Union{Bool,Nothing} = false,
    estimate_available_memory::Union{Bool,Nothing} = true,
    email_when_ready::Union{Bool,Nothing}=nothing,
    kwargs...,)::SessionId

    force_update_files = true
    store_logs_in_s3_orig = store_logs_in_s3
    try
        if print_logs
            # If logs need to be printed, ensure that we save logs in S3. If
            # store_logs_in_s3==False, then delete logs in S3 later
            store_logs_in_s3 = true
        end
        start_session(;cluster_name = cluster_name, nworkers = nworkers, release_resources_after = release_resources_after, 
                    print_logs = print_logs, store_logs_in_s3 = store_logs_in_s3, store_logs_on_cluster = store_logs_on_cluster, 
                    sample_rate = sample_rate, session_name = session_name, files = files, code_files = code_files, force_update_files = force_update_files,
                    pf_dispatch_table = pf_dispatch_table, using_modules = using_modules, url = url, branch = branch,
                    directory = directory, dev_paths = dev_paths, force_sync = force_sync, force_pull = force_pull, force_install = force_install, 
                    estimate_available_memory = estimate_available_memory, nowait = false, email_when_ready = email_when_ready, for_running = true)
    catch
        session_id = try
            get_session_id()
        catch
            nothing
        end
        if !isnothing(session_id)
            end_session(session_id, failed=true)
            if print_logs
                print_session_logs(session_id, cluster_name, delete_file=!store_logs_in_s3_orig)
            end
        end
        rethrow()
    finally
        session_id = try
            get_session_id()
        catch
            nothing
        end
        end_session(session_id, failed=false)
        if !isnothing(session_id)
            if print_logs
                print_session_logs(session_id, cluster_name, delete_file=!store_logs_in_s3_orig)
            end
        end    
    end
end

@specialize