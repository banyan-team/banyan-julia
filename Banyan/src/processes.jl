
function create_process(process_name, script; cron_schedule = "", creation_kwargs...)

    if startswith(script, "http://") || startswith(script, "https://")
        script = Downloads.download(script)
    end

    global BANYAN_JULIA_BRANCH_NAME
    global BANYAN_JULIA_PACKAGES

    session_configuration = Dict{String,Any}(
        "cluster_name" => get(creation_kwargs, :cluster_name, NOTHING_STRING),
        "num_workers" => get(creation_kwargs, :num_workers, 16),
        "sample_rate" => get(creation_kwargs, :sample_rate, nworkers),
        "release_resources_after" => get(creation_kwargs, :release_resources_after, 20),
        "return_logs" => get(creation_kwargs, :return_logs, false),
        "store_logs_in_s3" => get(creation_kwargs, :store_logs_in_s3, false),
        "store_logs_on_cluster" => store_logs_on_cluster,
        "log_initialization" => get(creation_kwargs, :log_initialization, false),
        "version" => get_julia_version(),
        "benchmark" => get(ENV, "BANYAN_BENCHMARK", "0")::String == "1",
        "main_modules" => filter(not_in_modules, get_loaded_packages()),
        "using_modules" => get(creation_kwargs, :using_modules, String[]),
        "reuse_resources" => get(creation_kwargs, :using_modules, false),
        "estimate_available_memory" => get(creation_kwargs, :estimate_available_memory, true),
        "language" => "jl"
    )

    session_name = get(creation_kwargs, :session_name, String)
    no_email = get(creation_kwargs, :no_email, Bool)
    email_when_ready = get(creation_kwargs, :email_when_ready, Bool)


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
    environment_info["force_sync"] = get(creation_kwargs, :force_sync, false)
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

    if get(creation_kwargs, :no_pf_dispatch_table, false)
        branch_to_use::String = get(ENV, "BANYAN_TESTING", "0")::String == "1" ? get_branch_name() : BANYAN_JULIA_BRANCH_NAME
        pf_dispatch_table = String[]
        for dir in BANYAN_JULIA_PACKAGES
            push!(pf_dispatch_table, "https://raw.githubusercontent.com/banyan-team/banyan-julia/$branch_to_use/$dir/res/pf_dispatch_table.toml")
        end
    end
    pf_dispatch_table_loaded = load_toml(get(creation_kwargs, :pf_dispatch_table, nothing))
    session_configuration["pf_dispatch_table"] = pf_dispatch_table_loaded

    
    # Construct cluster creation
    cluster_config = Dict{String,Any}(
        "cluster_name" => get(creation_kwargs, :name, nothing),
        "instance_type" => get(creation_kwargs, :instance_type, "m4.4xlarge"),
        "max_num_workers" => get(creation_kwargs, :max_num_workers, 2048),
        "initial_num_workers" => get(creation_kwargs, :initial_num_workers, 16),
        "min_num_workers" => get(creation_kwargs, :instance_type, 0),
        "aws_region" => get(creation_kwargs, :aws_region, nothing),
        "s3_read_write_resource" => get(creation_kwargs, :s3_read_write_resource, String),
        "scaledown_time" => scaledown_time,
        "recreate" => false,
        # We need to pass in the disk capacity in # of GiB and we do this by dividing the input
        # by size of 1 GiB and then round up. Then the backend will determine how to adjust the
        # disk capacity to an allowable increment (e.g., 1200 GiB or an increment of 2400 GiB
        # for AWS FSx Lustre filesystems)
        "disk_capacity" => disk_capacity == "auto" ? -1 : ceil(Int64, parse_bytes(disk_capacity) / 1.073741824e7)
    )

    iam_policy_arn = get(creation_kwargs, :iam_policy_arn, nothing)
    vpc_id = get(creation_kwargs, :vpc_id, nothing)
    subnet_id = get(creation_kwargs, :subnet_id, nothing)

    if haskey(c["aws"], "ec2_key_pair_name")
        cluster_config["ec2_key_pair"] = c["aws"]["ec2_key_pair_name"]
    end
    if !isnothing(iam_policy_arn)
        cluster_config["additional_policy"] = iam_policy_arn
    end
    if !isnothing(vpc_id)
        cluster_config["vpc_id"] = vpc_id
    end
    if !isnothing(subnet_id)
        cluster_config["subnet_id"] = subnet_id
    end

    creation_kwargs_dict = merge(session_configuration, cluster_config)

    creation_kwargs_dict["initial_num_workers"] = num_workers

    if !haskey(creation_kwargs_dict, "cluster_name")
        error("Cluster name is not specified")
    end

    for c in process_name
        if (isspace(c)) || !(isletter(c)) || !(isnum(c)) || !("_") || !("-")
            error("Process name must not include any white space and only alphanumeric characters, hyphens, or underscores.")
        end
    end

    creation_kwargs_dict["code_files"] = [process_name]

    code = read(script, String)

    # res_code is the code we are generating
    # Generate code to get session ID initially
    res_code = "self_session_id = get_session_id()\n"

    # Generate code to configure with Banyan credentials
    config = configure()
    user_id = config["banyan"]["user_id"]
    api_key = config["banyan"]["api_key"]
    res_code = res_code * "configure(user_id=" * user_id * ", api_key=" * api_key * ")\n"

    creation_kwargs_str = Banyan.to_jl_value_contents(creation_kwargs)
    res_code = res_code * "start_session(; Banyan.from_jl_value_contents$(creation_kwargs_str)...)\n" 

    res_code = res_code * "try\n"
    res_code *= code    

    res_code = res_code * "catch e \n"
    res_code = res_code * "end_session()\n"
    res_code = res_code * "rethrow(e)\n"
    res_code = res_code * "end\n"
    res_code = res_code * "end_session(self_session_id)\n"

    bucket_name = "banyan-scripts-$(get_session().organization_id)"
    # 1. Call list_buckets to check if the bucket_name exists
    # 2. If it doesn't, call create_bucket to create the bucket
    # 3. Convert String res_code to Vector{UInt8} (an array of bytes)
    # 4. Call put_object and pass in the bytes as the body and use the process name as the key

    # Create bucket if it does not exist
    buckets = S3.list_buckets()
    if !(bucket_name in buckets)
        S3.create_bucket(bucket_name)
    end 

    res_code_blob = Vector{UInt8}(res_code)
    params = Dict("Body" => res_code_blob)
    S3.put_object(bucket_name, process_name, params)

    aws_config = get_aws_config()
    aws_region = aws_config["region"]
    
    response = send_request_get_response(
        :create_process,
        Dict{String,Any}(
            "process_name" => process_name,
            "creation_kwargs" => creation_kwargs_dict,
            "cron_string" => cron_schedule,
            "aws_region" => aws_region
        ),
    )

end