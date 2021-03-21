using Base64
using JSON
using HTTP
using FileIO

function load_json(path::String)
    if startswith(path, "file://")
        JSON.parsefile(path[8:end])
    elseif startswith(path, "s3://")
        # TODO: Implement parsing JSON from S3
    elseif startswith(path, "http://") || startswith(path, "https://")
        JSON.parse(HTTP.get(path))
    else
        error("Path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

function merge_with(banyanfile_so_far::Dict, banyanfile::Dict, selector::Function)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    collect(union(Set(so_far), Set(curr)))
end

function merge_paths_with(banyanfile_so_far::Dict, banyanfile::Dict, selector::Function)
    # Merge where we combine arrays by taking unions of their unique elements
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    deduplicated_absolute_locations = collect(union(Set(so_far), Set(curr)))
    deduplicated_relative_locations = unique(loc->basename(loc), vcat(so_far, curr))
    if deduplicated_relative_locations < deduplicated_absolute_locations
        error("Files and scripts must have unique base names: $so_far and $curr have the same base name")
    else
        deduplicated_absolute_locations
    end
end

function keep_same(banyanfile_so_far::Dict, banyanfile::Dict, selector::Function)
    so_far = selector(banyanfile_so_far)
    curr = selector(banyanfile)
    if so_far != curr
        error("$so_far does not match $curr in included Banyanfiles")
    end
end

function merge_banyanfile_with!(banyanfile_so_far::Dict, banyanfile_path::String, for_cluster_or_job::Symbol)
    banyanfile = load_json(banyanfile_path)

    # Merge with all included
    for included in banyanfile["include"]
        merge_banyanfile_with!(banyanfile_so_far, included, for_cluster_or_job)
    end
    banyanfile_so_far["include"] = []

    # Merge with rest of what is in this banyanfile

    if for_cluster_or_job == :cluster
        # Merge language
        keep_same(banyanfile_so_far, banyanfile, b->b["require"]["language"])

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
            b->b["require"]["job"]["code"]
        )
    else
        error("Expected for_cluster_or_job to be either :cluster or :job")
    end
end


function load_banyanfile(banyanfile_path::String = "res/Banyanfile.json")
    # TODO: Implement this to load Banyanfile, referenced pt_lib_info, pt_lib,
    # code files

    # Load Banyanfile and merge with all included
    banyanfile = load_json(banyanfile_path)
    for included in banyanfile["included"]
        merge_banyanfile_with!(banyanfile, included, :cluster)
    end

    # TODO: Use single Banyanfile to upload to S3 and prepare for upload

    open(banyanfile_path, "r") do f
       global banyanfile
       banyanfile = JSON.parse(read(f, String))
    end
    # Load all included banyanfiles
    include = []
    for inc in banyanfile["include"]
        if inc isa String
            # TODO: Determine wheter to download or is local path
            # download(inc)
            open(inc, "r") do f
                append!(include, JSON.parse(read(f, String)))
            end
        else
            append(!include, inc)
        end
    end
    # Load pt_lib_info if path provided
    pt_lib_info = banyanfile["require"]["cluster"]["pt_lib_info"]
    if pt_lib_info isa String
        # TODO: Determine whether to download or is local path
            # download(pt_lib_info)
        open(pt_lib_info, "r") do f
            banyanfile["require"]["cluster"]["pt_lib_info"] = JSON.parse(read(f, String))
        end
    end
    return banyanfile
end

# function load_configfile(configfile_path::String)
#     configfile_path = "res/BanyanConfig.json"  # TODO: Remove this
#     configfile = Dict()
#     open(configfile_path, "r") do f
#         global configfile
#         configfile = JSON.parse(read(f, String))
#     end
#     return configfile
# end


# Required: cluster_id, num_nodes
function create_cluster(
    name::String;
    instance_type::String = "m4.4xlarge",
    max_num_nodes::Int = 8,
    banyanfile_path::String = nothing,
    iam_policy_arn::String = nothing,
    s3_bucket_arn::String = nothing;
    kwargs...
)
    @debug "Creating cluster"

    # Configure using parameters
    c = configure(;require_ec2_key_pair_name = true, kwargs...)

    # Construct cluster creation
    cluster_config = Dict(
        "cluster_id" => name,
        "instance_type" => instance_type, #"t3.large", "c5.2xlarge"
        "num_nodes" => max_num_nodes,
        "ec2_key_pair" => c["aws"]["ec2_key_pair_name"],
        "aws_region" => get_aws_config_region()
    )
    if !isnothing(banyanfile_path)
        # TODO: Load Banyanfile
        cluster_config["banyanfile"] = load_banyanfile(banyanfile_path)
    end
    if !isnothing(iam_policy_arn)
        cluster_config["additional_policy"] = iam_policy_arn # "arn:aws:s3:::banyanexecutor*"
    end
    if !isnothing(s3_bucket_arn)
        cluster_config["s3_read_write_resource"] = s3_bucket_arn
    end
  
    # Send request to create cluster
    send_request_get_response(
        :create_cluster,
        cluster_config,
    )
end

function destroy_cluster(name::String; kwargs...)
    @debug "Destroying cluster"
    configure(;kwargs...)
    send_request_get_response(
        :destroy_cluster,
        Dict("cluster_id" => name)
    )
end

# TODO: Update website display
# TODO: Implement load_banyanfile
function update_cluster(
    ;name::String = nothing,
    max_num_nodes::Int = nothing,
    banyanfile_path::String = nothing,
    iam_policy_arn::String = nothing,
    s3_bucket_arn::String = nothing;
    kwargs...
)
    @debug "Updating cluster"

    # Configure
	configure(;kwargs...)
	cluster_name = if isnothing(cluster_name) first(keys(list_clusters())) else cluster_name end

     # Require restart: pcluster_additional_policy, s3_read_write_resource, num_nodes
     # No restart: Banyanfile

    cluster_config = Dict("cluster_id" => cluster_name)
    if !isnothing(max_num_nodes)
        cluster_config["max_num_nodes"] = max_num_nodes
    end
    if !isnothing(banyanfile_path)
        # TODO: Load Banyanfile
        cluster_config["banyanfile"] = load_banyanfile(banyanfile_path)
    end
    if !isnothing(iam_policy_arn)
        cluster_config["additional_policy"] = iam_policy_arn # "arn:aws:s3:::banyanexecutor*"
    end
    if !isnothing(s3_bucket_arn)
        cluster_config["s3_read_write_resource"] = s3_bucket_arn
    end

    send_request_get_response(
        :update_cluster,
        cluster_config
    )
end

struct Cluster
    name::String
    status::Symbol
    num_jobs_running::Int32
end

function list_clusters(;kwargs...)
    @debug "Destroying cluster"
    configure(;kwargs...)
    response = send_request_get_response(
        :describe_clusters, Dict()
    )
    clusters = []
    for (name, c) in response["clusters"]
        push!(clusters, Cluster(
            c["name"],
            # TODO: Parse status
            c["status"],
            c["num_jobs"],
        ))
    end
end

get_cluster(name::String; kwargs...) = list_clusters(name; kwargs...)[name]