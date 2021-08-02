
function load_json(path::String)
    if startswith(path, "file://")
        if !isfile(path[8:end])
            error("File $path does not exist")
        end
        JSON.parsefile(path[8:end])
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        # JSON.parsefile(S3Path(path, config=get_aws_config()))
    elseif startswith(path, "http://") || startswith(path, "https://")
	JSON.parse(String(HTTP.get(path).body))
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

# Loads file into String and returns
function load_file(path::String)
    if startswith(path, "file://")
        if !isfile(path[8:end])
            error("File $path does not exist")
        end
        String(read(open(path[8:end])))
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        String(read(S3Path(path)))
    elseif startswith(path, "http://") || startswith(path, "https://")
        String(HTTP.get(path).body)
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

function merge_with(banyanfile_so_far::Dict, banyanfile::Dict, selector::Function)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    Base.collect(union(Set(so_far), Set(curr)))
end

function merge_paths_with(
    banyanfile_so_far::Dict,
    banyanfile_path::String,  # TODO: Remove this unused parameter
    banyanfile::Dict,
    selector::Function,
)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    # curr = [getnormpath(banyanfile_path, p) for p in selector(banyanfile)]
    curr = selector(banyanfile)
    deduplicated_absolute_locations = collect(union(Set(so_far), Set(curr)))
    deduplicated_relative_locations = unique(loc -> basename(loc), vcat(so_far, curr))
    # TODO: Use the normalize path here
    if length(deduplicated_relative_locations) < length(deduplicated_absolute_locations)
        error(
            "Files and scripts must have unique base names: $so_far and $curr have the same base name",
        )
    else
        deduplicated_absolute_locations
    end
end

function keep_same(banyanfile_so_far::Dict, banyanfile::Dict, selector::Function)
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    if !isnothing(so_far) && !isnothing(curr) && so_far != curr
        @warn "$so_far does not match $curr in included Banyanfiles"
    end
    isnothing(so_far) ? curr : so_far
end

function keep_same_path(
    banyanfile_so_far::Dict,
    banyanfile_path::String,  # TODO: Remove unused parameter
    banyanfile::Dict,
    selector::Function,
)
    so_far = selector(banyanfile_so_far)
    curr_selected = selector(banyanfile)
    # TODO: Use the normalized path here
    curr = isnothing(curr_selected) ? nothing : curr_selected #getnormpath(banyanfile_path, curr_selected)
    if !isnothing(so_far) && !isnothing(curr) && so_far != curr
        @warn "$so_far does not match $curr in included Banyanfiles"
    end
    isnothing(so_far) ? curr : so_far
end

function getnormpath(banyanfile_path, p)
    if startswith(p, "file://")
        prefix, suffix = split(banyanfile_path, "://")
        banyanfile_location_path = dirname(suffix)
        @debug banyanfile_location_path
        prefix * "://" * normpath(banyanfile_location_path, last(split(p, "://")))
    else
        p
    end
end

function merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)
    # Populate with defaults
    mergewith!((a, b) -> a, banyanfile, Dict("include" => [], "require" => Dict()))
    mergewith!(
        (a, b) -> a,
        banyanfile["require"],
        Dict("language" => "jl", "cluster" => Dict(), "job" => Dict()),
    )
    # TODO: Fix `update_cluster`
    banyan_dir = dirname(dirname(pathof(Banyan)))
    mergewith!(
        (a, b) -> a,
        banyanfile["require"]["cluster"],
        Dict(
            "files" => [],
            "scripts" => [],
            "packages" => [],
            "pt_lib" => "file://$banyan_dir/res/pt_lib.jl",
            "pt_lib_info" => "file://$banyan_dir/res/pt_lib_info.json",
        ),
    )
    mergewith!((a, b) -> a, banyanfile["require"]["job"], Dict("code" => []))

    # Ensure all paths are normalized
    banyanfile["include"] = [getnormpath(banyanfile_path, f) for f in banyanfile["include"]]
    banyanfile["require"]["cluster"]["files"] =
        [getnormpath(banyanfile_path, f) for f in banyanfile["require"]["cluster"]["files"]]
    banyanfile["require"]["cluster"]["scripts"] = [
        getnormpath(banyanfile_path, f) for
        f in banyanfile["require"]["cluster"]["scripts"]
    ]
    banyanfile["require"]["cluster"]["pt_lib"] =
        isnothing(banyanfile["require"]["cluster"]["pt_lib"]) ? nothing :
        getnormpath(banyanfile_path, banyanfile["require"]["cluster"]["pt_lib"])
    banyanfile["require"]["cluster"]["pt_lib_info"] =
        isnothing(banyanfile["require"]["cluster"]["pt_lib"]) ? nothing :
        getnormpath(banyanfile_path, banyanfile["require"]["cluster"]["pt_lib_info"])
end

function merge_banyanfile_with!(
    banyanfile_so_far::Dict,
    banyanfile_path::String,
    for_cluster_or_job::Symbol,
    for_creation_or_update::Symbol,
)
    # Load Banyanfile to merge with
    banyanfile = load_json(banyanfile_path)

    # Merge Banyanfile with defaults
    merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)

    # Merge with all included
    for included in banyanfile["include"]
        merge_banyanfile_with!(
            banyanfile_so_far,
            included,
            for_cluster_or_job,
            for_creation_or_update,
        )
    end
    banyanfile_so_far["include"] = []

    # Merge with rest of what is in this banyanfile

    if for_cluster_or_job == :cluster
        if for_creation_or_update == :creation
            # Merge language
            keep_same(banyanfile_so_far, banyanfile, b -> b["require"]["language"])
        else
            @warn "Ignoring language"
        end

        # Merge files, scripts, packages
        banyanfile_so_far["require"]["cluster"]["files"] = merge_paths_with(
            banyanfile_so_far,
            banyanfile_path,
            banyanfile,
            b -> b["require"]["cluster"]["files"],
        )
        banyanfile_so_far["require"]["cluster"]["scripts"] = merge_paths_with(
            banyanfile_so_far,
            banyanfile_path,
            banyanfile,
            b -> b["require"]["cluster"]["scripts"],
        )
        banyanfile_so_far["require"]["cluster"]["packages"] = merge_with(
            banyanfile_so_far,
            banyanfile,
            b -> b["require"]["cluster"]["packages"],
        )

        # Merge pt_lib_info and pt_lib
        banyanfile_so_far["require"]["cluster"]["pt_lib_info"] = keep_same_path(
            banyanfile_so_far,
            banyanfile_path,
            banyanfile,
            b -> b["require"]["cluster"]["pt_lib_info"],
        )
        banyanfile_so_far["require"]["cluster"]["pt_lib"] = keep_same_path(
            banyanfile_so_far,
            banyanfile_path,
            banyanfile,
            b -> b["require"]["cluster"]["pt_lib"],
        )
    elseif for_cluster_or_job == :job
        # Merge code
        banyanfile_so_far["require"]["job"]["code"] =
            merge_with(banyanfile_so_far, banyanfile, b -> b["require"]["job"]["code"])
        # TODO: If code is too large, upload to S3 bucket and replace code with
        # an include statement
    else
        error("Expected for_cluster_or_job to be either :cluster or :job")
    end
end

function upload_banyanfile(
    banyanfile::Union{String, Dict},
    s3_bucket_arn::String,
    cluster_name::String,
    for_creation_or_update::Symbol
)

    # Get s3 bucket name from arn
    s3_bucket_name = s3_bucket_arn_to_name(s3_bucket_arn)

    # Validate that s3 bucket exists
    s3_exists(get_aws_config(), s3_bucket_name, "")

    # Check if postinstall script exists. If so, append.
    post_install_script_name = "banyan_$(cluster_name)_script.sh"
    post_install_script =  ""
    update_script_name = "banyan_$(cluster_name)_update_script.sh"
    update_script = "#!/bin/bash\n. \"/etc/parallelcluster/cfnconfig\"\n"
    try
        post_install_script = String(s3_get(get_aws_config(), s3_bucket_name, post_install_script_name))
    catch
        if for_creation_or_update == :update
            error("Script should already exist for created cluster or cluster to update does not exist")
        end
        @debug "Creating new post install script for cluster"
    end

    # Load Banyanfile and merge with all included
    if typeof(banyanfile) == String
        banyanfile_path = banyanfile
        banyanfile = load_json(banyanfile_path)
    else
        banyanfile_path = "file://"
    end
    merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)
    for included in banyanfile["include"]
        merge_banyanfile_with!(banyanfile, included, :cluster, for_creation_or_update)
    end

    # Load pt_lib_info if path provided
    pt_lib_info = banyanfile["require"]["cluster"]["pt_lib_info"]
    @debug pt_lib_info
    pt_lib_info = if pt_lib_info isa String
        load_json(pt_lib_info)
    else
        pt_lib_info
    end

    files = banyanfile["require"]["cluster"]["files"]
    scripts = banyanfile["require"]["cluster"]["scripts"]
    packages = banyanfile["require"]["cluster"]["packages"]
    pt_lib = banyanfile["require"]["cluster"]["pt_lib"]
    pt_lib = isnothing(pt_lib) ? [] : [pt_lib]

    #if isnothing(pt_lib)
    #    error("No pt_lib.jl provided")
    #end
    #if isnothing(pt_lib_info)
    #    error("No pt_lib_info.json provided")
    #end

    # Upload all files, scripts, and pt_lib to s3 bucket
    for f in vcat(files, scripts, pt_lib)
        s3_put(get_aws_config(), s3_bucket_name, basename(f), load_file(f))
    end

    bucket = s3_bucket_name
    region = get_aws_config_region()

    # Append to post-install script downloading files, scripts, pt_lib onto cluster
    update_script *= "if [ \"\${cfn_node_type}\" == MasterServer ];\nthen\n"
    for f in vcat(files, scripts, pt_lib)
        update_script *=
            "sudo su - ec2-user -c \"aws s3 cp s3://" *
            s3_bucket_name *
            "/" *
            basename(f) *
            " /home/ec2-user/\"\n"
    end

    # Append to post-install script installing Julia dependencies
    for pkg in packages
        pkg_spec = split(pkg, "@")
        if length(pkg_spec) == 1
            update_script *= "sudo su - ec2-user -c \"julia/bin/julia --project -e 'using Pkg; Pkg.add(name=\\\"$pkg\\\")' &>> setup_log.txt \"\n"
        elseif length(pkg_spec) == 2
            name, version = pkg_spec
            update_script *= "sudo su - ec2-user -c \"julia/bin/julia --project -e 'using Pkg; Pkg.add(name=\\\"$name\\\", version=\\\"$version\\\")' &>> setup_log.txt \"\n"
        end
    end
    update_script *= "fi\n"

    # Append to post-install script running scripts onto cluster
    for script in scripts
        fname = basename(script)
        update_script *= "sudo su - ec2-user -c \"bash /home/ec2-user/$fname\"\n"
    end

    # Upload post_install script to s3 bucket
    #post_install_script *=
    #    "touch /home/ec2-user/update_finished\n" *
    #    "aws s3 cp /home/ec2-user/update_finished " *
    #    "s3://" *
    #    s3_bucket_name *
    #    "/\n"
    post_install_script *= "\n" * update_script
    # TODO: Fix this
    s3_put(get_aws_config(), s3_bucket_name, post_install_script_name, post_install_script)
    if for_creation_or_update == :update
        update_script *=
            "touch /home/ec2-user/update_finished\n" *
            "aws s3 cp /home/ec2-user/update_finished s3://$(s3_bucket_name)/\n"
        s3_put(get_aws_config(), s3_bucket_name, update_script_name, update_script)
    end
    @debug post_install_script
    @debug pt_lib_info
    return pt_lib_info
end

# Required: cluster_name
function create_cluster(;
    name::Union{String,Nothing} = nothing,
    instance_type::Union{String,Nothing} = "m4.4xlarge",
    max_num_nodes::Union{Int,Nothing} = 8,
    banyanfile_path::String = nothing,
    iam_policy_arn::Union{String,Nothing} = nothing,
    s3_bucket_arn::Union{String,Nothing} = nothing,
    s3_bucket_name::Union{String,Nothing} = nothing,
    vpc_id = nothing,
    subnet_id = nothing,
    kwargs...,
)
    @debug "Creating cluster"

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    clusters = get_clusters()
    if haskey(clusters, name)
        if clusters[name][status] == "terminated"
            @warn "Cluster configuration with name $name already exists. Ignoring new configuration and re-creating cluster."
            send_request_get_response(:create_cluster, Dict("cluster_name" => name, "recreate" => true))
            return
        else
            error("Cluster with name $name already exists")
        end
    end

    # Construct arguments

    # Configure using parameters
    c = configure(; require_ec2_key_pair_name = true, kwargs...)
    name = if !isnothing(name)
        name
    else
        "banyan-cluster-" * randstring(6)
    end

    if isnothing(s3_bucket_arn) && isnothing(s3_bucket_name)
        s3_bucket_arn =
            "arn:aws:s3:::banyan-cluster-data-" * name * "-" * bytes2hex(rand(UInt8, 4))
        s3_bucket_name = last(split(s3_bucket_arn, ":"))
        s3_create_bucket(get_aws_config(), s3_bucket_name)
    elseif isnothing(s3_bucket_arn)
        s3_bucket_arn = "arn:aws:s3:::$s3_bucket_name*"
    elseif isnothing(s3_bucket_name)
        s3_bucket_name = last(split(s3_bucket_arn, ":"))
    end
    if !(s3_bucket_name in s3_list_buckets(get_aws_config()))
        error("Bucket $s3_bucket_name does not exist in connected AWS account")
    end

    # Construct cluster creation
    cluster_config = Dict(
        "cluster_name" => name,
        "instance_type" => instance_type,
        "num_nodes" => max_num_nodes,
        "ec2_key_pair" => c["aws"]["ec2_key_pair_name"],
        "aws_region" => get_aws_config_region(),
        "s3_read_write_resource" => s3_bucket_arn,
	"recreate" => false
    )
    if !isnothing(banyanfile_path)
        pt_lib_info = upload_banyanfile(banyanfile_path, s3_bucket_arn, name, :creation)
        cluster_config["pt_lib_info"] = pt_lib_info
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

    # Send request to create cluster
    send_request_get_response(:create_cluster, cluster_config)

    return Cluster(
	name,
	:creating,
	0,
	s3_bucket_arn
    )
end

function destroy_cluster(name::String; kwargs...)
    @debug "Destroying cluster"
    configure(; kwargs...)
    send_request_get_response(:destroy_cluster, Dict{String,Any}("cluster_name" => name))
end

function delete_cluster(name::String; kwargs...)
    @debug "Deleting cluster"
    configure(; kwargs...)
    send_request_get_response(:destroy_cluster, Dict{String, Any}("cluster_name" => name, "permanently_delete" => true))
end

# TODO: Update website display
# TODO: Implement load_banyanfile
function update_cluster(;
    name::Union{String, Nothing} = nothing,
    banyanfile_path::Union{String, Nothing} = nothing,
    banyanfile::Union{Dict, Nothing} = nothing,
    reinstall_julia = false,
    force = false,
    kwargs...,
)
    @info "Updating cluster"

    # Configure
    configure(; kwargs...)
    cluster_name = if isnothing(name)
        clusters = get_clusters()
        if length(clusters) == 0
            error("Failed to update cluster: you don't have any clusters created")
        end
        first(keys(clusters))
    else
        name
    end

    update_args = Dict(
        "cluster_name" => cluster_name,
	"reinstall_julia" => reinstall_julia
    )

    # Force by setting cluster to running
    if force
        assert_cluster_is_ready(name = name)
    end

    if !isnothing(banyanfile_path)
        # Retrieve the location of the current post_install script in S3 and upload
        # the updated version to the same location
        s3_bucket_arn = s3_bucket_name_to_arn(get_cluster(name).s3_bucket_arn)

        # Upload to S3
        pt_lib_info = upload_banyanfile(
            banyanfile_path,
            s3_bucket_arn,
            cluster_name,
            :update
        )
	update_args["pt_lib_info"] = pt_lib_info
	update_args["banyanfile"] = banyanfile
    elseif !isnothing(banyanfile)
        # Retrieve the location of the current post_install script in S3 and upload
	# the updated version to the same location
	s3_bucket_arn = s3_bucket_name_to_arn(get_cluster(name).s3_bucket_arn)
        
        # Upload to S3
        pt_lib_info = upload_banyanfile(
            banyanfile,
            s3_bucket_arn,
            cluster_name,
            :update
        )
        update_args["pt_lib_info"] = pt_lib_info
        update_args["banyanfile"] = banyanfile
    end
    send_request_get_response(
        :update_cluster,
        update_args,
    )
end

function assert_cluster_is_ready(; name::String, kwargs...)
    @info "Setting cluster status to running"

    # Configure
    configure(; kwargs...)

    send_request_get_response(:set_cluster_ready, Dict{String,Any}("cluster_name" => name))
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
    elseif status == "terminated"
        :terminated
    else
        error("Unexpected status ", status)
    end

function get_clusters(; kwargs...)
    @debug "Downloading description of clusters"
    configure(; kwargs...)
    response = send_request_get_response(:describe_clusters, Dict{String,Any}())
    if is_debug_on()
        @show response
    end
    Dict(
        name => Cluster(
            name,
            parsestatus(c["status"]),
            c["num_jobs"],
            c["s3_read_write_resource"],
        ) for (name, c) in response["clusters"]
    )
end

get_cluster(name::String, kwargs...) = get_clusters(;kwargs...)[name]
get_cluster() = get_cluster(name=get_cluster_name())
