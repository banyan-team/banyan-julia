

function load_json(path::String)
    if startswith(path, "file://")
        JSON.parsefile(path[8:end])
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        # JSON.parsefile(S3Path(path, config=get_aws_config()))
    elseif startswith(path, "http://") || startswith(path, "https://")
        JSON.parse(HTTP.get(path).body)
    else
        error(
            "Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"",
        )
    end
end

# Loads file into String and returns
function load_file(path::String)
    if startswith(path, "file://")
        String(read(open(path[8:end])))
    elseif startswith(path, "s3://")
        String(read(S3Path(path)))
    elseif startswith(path, "http://") || startswith(path, "https://")
        String(HTTP.get(path).body)
    else
        error(
            "Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"",
        )
    end
end

function merge_with(
    banyanfile_so_far::Dict,
    banyanfile::Dict,
    selector::Function,
)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    collect(union(Set(so_far), Set(curr)))
end

function merge_paths_with(
    banyanfile_so_far::Dict,
    banyanfile::Dict,
    selector::Function,
)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    deduplicated_absolute_locations = collect(union(Set(so_far), Set(curr)))
    deduplicated_relative_locations =
        unique(loc -> basename(loc), vcat(so_far, curr))
    if deduplicated_relative_locations < deduplicated_absolute_locations
        error(
            "Files and scripts must have unique base names: $so_far and $curr have the same base name",
        )
    else
        deduplicated_absolute_locations
    end
end

function keep_same(
    banyanfile_so_far::Dict,
    banyanfile::Dict,
    selector::Function,
)
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    if so_far != curr
        error("$so_far does not match $curr in included Banyanfiles")
    end
end

function merge_banyanfile_with!(
    banyanfile_so_far::Dict,
    banyanfile_path::String,
    for_cluster_or_job::Symbol,
)
    banyanfile = load_json(banyanfile_path)

    # Merge with all included
    for included in banyanfile["include"]
        merge_banyanfile_with!(banyanfile_so_far, included, for_cluster_or_job)
    end
    banyanfile_so_far["include"] = []

    # Merge with rest of what is in this banyanfile

    if for_cluster_or_job == :cluster
        # Merge language
        keep_same(banyanfile_so_far, banyanfile, b -> b["require"]["language"])

        # Merge files, scripts, packages
        banyanfile_so_far["require"]["cluster"]["files"] = merge_paths_with(
            banyanfile_so_far,
            banyanfile,
            b -> b["require"]["cluster"]["files"],
        )
        banyanfile_so_far["require"]["cluster"]["scripts"] = merge_paths_with(
            banyanfile_so_far,
            banyanfile,
            b -> b["require"]["cluster"]["scripts"],
        )
        banyanfile_so_far["require"]["cluster"]["packages"] = merge_with(
            banyanfile_so_far,
            banyanfile,
            b -> b["require"]["cluster"]["packages"],
        )

        # NOTE: We use whatever the top-level value of pt_lib_info and pt_lib are
        # # Merge pt_lib_info and pt_lib
        # keep_same(banyanfile_so_far, banyanfile, b->b["require"]["cluster"]["pt_lib_info"])
        # keep_same(banyanfile_so_far, banyanfile, b->b["require"]["cluster"]["pt_lib"])
    elseif for_cluster_or_job == :job
        # Merge code
        banyanfile_so_far["require"]["job"]["code"] = merge_with(
            banyanfile_so_far,
            banyanfile,
            b -> b["require"]["job"]["code"],
        )
    else
        error("Expected for_cluster_or_job to be either :cluster or :job")
    end
end

function upload_banyanfile(banyanfile_path::String, s3_bucket_arn::String, cluster_id::String)
    # TODO: Implement this to load Banyanfile, referenced pt_lib_info, pt_lib,
    # code files

    # Load Banyanfile and merge with all included
    banyanfile = load_json(banyanfile_path)
    for included in banyanfile["include"]
        merge_banyanfile_with!(banyanfile, included, :cluster)
    end

    # Load pt_lib_info if path provided
    pt_lib_info = banyanfile["require"]["cluster"]["pt_lib_info"]
    pt_lib_info = if pt_lib_info isa String
        load_json(pt_lib_info)
    else
        pt_lib_info
    end

    files = banyanfile["require"]["cluster"]["files"]
    scripts = banyanfile["require"]["cluster"]["scripts"]
    packages = banyanfile["require"]["cluster"]["packages"]
    pt_lib = banyanfile["require"]["cluster"]["pt_lib"]

    # Upload all files, scripts, and pt_lib to s3 bucket
    s3_bucket_name = last(split(s3_bucket_arn, ":"))
    if endswith(s3_bucket_name, "/")
        s3_bucket_name = s3_bucket_name[1:end-1]
    elseif endswith(s3_bucket_name, "/*")
        s3_bucket_name = s3_bucket_name[1:end-2]
    elseif endswith(s3_bucket_name, "*")
        s3_bucket_name = s3_bucket_name[1:end-1]
    end
    #AWSCore.set_debug_level(4)
    for f in vcat(files, scripts, pt_lib)
        println(s3_bucket_name)
        println(basename(f))
        s3_put(get_aws_config(), s3_bucket_name, basename(f), load_file(f))
    end

    # Create post-install script with base commands
    code = """
#!/bin/bash
sudo yum update -y &>> setup_log.txt
wget https://julialang-s3.julialang.org/bin/linux/x64/1.5/julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
tar zxvf julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
rm julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
julia-1.5.3/bin/julia --project -e 'using Pkg; Pkg.add([\"AWSCore\", \"AWSSQS\", \"HTTP\", \"Dates\", \"JSON\", \"MPI\", \"Serialization\"]); ENV[\"JULIA_MPIEXEC\"]=\"srun\"; ENV[\"JULIA_MPI_LIBRARY\"]=\"/opt/amazon/openmpi/lib64/libmpi\"; Pkg.build(\"MPI\"; verbose=true)' &>> setup_log.txt
aws s3 cp s3://banyanexecutor /home/ec2-user --recursive
    """

    # Append to post-install script downloading files, scripts, pt_lib onto cluster
    for f in vcat(files, scripts, pt_lib)
        code *=
            "aws s3 cp s3://" * s3_bucket_name * "/" *
            basename(f) *
            " /home/ec2-user/\n"
    end

    # Append to post-install script running scripts onto cluster
    for script in scripts
        fname = basename(f)
        code *= "bash /home/ec2-user/$fname\n"
    end

    # Append to post-install script installing Julia dependencies
    for pkg in packages
        code *= "julia-1.5.3/bin/julia --project -e 'using Pkg; Pkg.add([\"$pkg\"])' &>> setup_log.txt\n"
    end

    println(code)

    # Upload post_install script to s3 bucket
    post_install_script = "banyan_$cluster_id" * "_script.sh"
    s3_put(get_aws_config(), s3_bucket_name, post_install_script, code)

    return pt_lib_info
end

# Required: cluster_id, num_nodes
function create_cluster(;
    name::String = nothing,
    instance_type::String = "m4.4xlarge",
    max_num_nodes::Int = 8,
    banyanfile_path::String = nothing,
    iam_policy_arn::String = nothing,
    s3_bucket_arn::String = nothing,
    kwargs...,
)
    @debug "Creating cluster"

    # Configure using parameters
    c = configure(; require_ec2_key_pair_name = true, kwargs...)
    name = if !isnothing(name)
        name
    else
        "banyan-cluster-" * randstring(6)
    end
    if isnothing(s3_bucket_arn)
        s3_bucket_arn = "arn:aws:s3:::banyan-cluster-data-" * name
        s3_bucket_name = last(split(s3_bucket_arn, ":"))
        s3_create_bucket(get_aws_config(), s3_bucket_name)
    end

    # Construct cluster creation
    cluster_config = Dict(
        "cluster_id" => name,
        "instance_type" => instance_type, #"t3.large", "c5.2xlarge"
        "num_nodes" => max_num_nodes,
        "ec2_key_pair" => c["aws"]["ec2_key_pair_name"],
        "aws_region" => get_aws_config_region(),
        "s3_read_write_resource" => s3_bucket_arn,
    )
    if !isnothing(banyanfile_path)
        pt_lib_info = upload_banyanfile(banyanfile_path, s3_bucket_arn, name)
        cluster_config["pt_lib_info"] = pt_lib_info
    end
    if !isnothing(iam_policy_arn)
        cluster_config["additional_policy"] = iam_policy_arn # "arn:aws:s3:::banyanexecutor*"
    end

    # Send request to create cluster
    send_request_get_response(:create_cluster, cluster_config)
end

function destroy_cluster(name::String; kwargs...)
    @debug "Destroying cluster"
    configure(; kwargs...)
    send_request_get_response(:destroy_cluster, Dict("cluster_id" => name))
end

# TODO: Update website display
# TODO: Implement load_banyanfile
function update_cluster(;
    name::String = nothing,
    banyanfile_path::String = nothing,
    kwargs...,
)
    @debug "Updating cluster"

    # Configure
    configure(; kwargs...)
    cluster_name = if isnothing(name)
        clusters = get_clusters()
        if length(clusters) == 0
            error("Failed to create job: you don't have any clusters created")
        end
        first(keys(clusters))
    else
        name
    end

    # Require restart: pcluster_additional_policy, s3_read_write_resource, num_nodes
    # No restart: Banyanfile

    if !isnothing(banyanfile_path)
        # Retrieve the location of the current post_install script in S3 and upload
        # the updated version to the same location
        s3_bucket_arn = get_cluster(name).s3_bucket_arn
	if endswith(s3_bucket_arn, "/")
            s3_bucket_arn = s3_bucket_arn[1:end-1]
        elseif endswith(s3_bucket_arn, "/*")
            s3_bucket_arn = s3_bucket_arn[1:end-2]
        elseif endswith(s3_bucket_arn, "*")
	    s3_bucket_arn = s3_bucket_arn[1:end-1]
	end

        # Upload to S3
        pt_lib_info = upload_banyanfile(banyanfile_path, s3_bucket_arn, cluster_name)
        println("pt_lib_info:")
        println(pt_lib_info)

        # Upload pt_lib_info
        send_request_get_response(
            :update_cluster,
            Dict(
                "cluster_id" => name,
                "pt_lib_info" => pt_lib_info
            ),
        )
    end
end

struct Cluster
    name::String
    status::Symbol
    num_jobs_running::Int32
    s3_bucket_arn::String
end

parsestatus(status) =
    if status == "creating"
        :creating
    elseif status == "destroying"
        :destroying
    elseif status == "updating"
        :updating
    elseif status == "failed"
        :failed
    elseif status == "starting"
        :starting
    elseif status == "stopped"
        :stopped
    elseif status == "running"
        :running
    else
        error("Unexpected status")
    end

function get_clusters(; kwargs...)
    @debug "Destroying cluster"
    configure(; kwargs...)
    response =
        send_request_get_response(:describe_clusters, Dict{String,Any}())
    clusters = []
    Dict(
        name => Cluster(
            name,
            parsestatus(c["status"]),
            c["num_jobs"],
            c["s3_read_write_resource"],
        ) for (name, c) in response["clusters"]
    )
end

get_cluster(name::String; kwargs...) = get_clusters(; kwargs...)[name]
