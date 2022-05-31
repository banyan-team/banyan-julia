struct Cluster
    name::String
    status::Symbol
    status_explanation::String
    s3_bucket_arn::String
    organization_id::String
    curr_cluster_instance_id::String
end

# Process-local dictionary mapping from cluster names to instances of `Cluster`
# Might contain stale information
global clusters = Dict{String,Cluster}()

function get_clusters_dict()::Dict{String,Cluster}
    global clusters
    clusters
end

@nospecialize

function create_cluster(;
    name::Union{String,Nothing} = nothing,
    instance_type::Union{String,Nothing} = "m4.4xlarge",
    max_num_workers::Union{Int,Nothing} = 2048,
    initial_num_workers::Union{Int,Nothing} = 16,
    min_num_workers::Union{Int,Nothing} = 0,
    iam_policy_arn::Union{String,Nothing} = nothing,
    s3_bucket_arn::Union{String,Nothing} = nothing,
    s3_bucket_name::Union{String,Nothing} = nothing,
    disk_capacity = "1200 GiB",
    scaledown_time = 25,
    region = nothing,
    vpc_id = nothing,
    subnet_id = nothing,
    nowait=false,
    kwargs...,
)

    # Configure using parameters
    c = configure(; kwargs...)
    
    clusters = get_clusters(; kwargs...)
    if isnothing(name)
        name = "Cluster " * string(length(clusters) + 1)
    end
    if isnothing(region)
        region = get_aws_config_region()
    end

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if haskey(clusters, name)
        if clusters[name].status == :terminated
            @info "Started re-creating cluster named $name"
            send_request_get_response(
                :create_cluster,
                Dict("cluster_name" => name, "recreate" => true),
            )
            if !nowait
                wait_for_cluster(name; kwargs...)
            end
            return get_cluster(name; kwargs...)
        else
            error("Cluster with name $name already exists and its current status is $(string(clusters[name].status))")
        end
    end

    # Construct arguments
    if !isnothing(s3_bucket_name)
        s3_bucket_arn = "arn:aws:s3:::$s3_bucket_name*"
    elseif !isnothing(s3_bucket_arn)
        s3_bucket_name = split(s3_bucket_arn, ":")[end]
    end
    if isnothing(s3_bucket_arn)
        s3_bucket_arn = ""
    elseif !(s3_bucket_name in s3_list_buckets(get_aws_config()))
        error("Bucket $s3_bucket_name does not exist in the connected AWS account")
    end

    # Construct cluster creation
    cluster_config = Dict{String,Any}(
        "cluster_name" => name,
        "instance_type" => instance_type,
        "max_num_workers" => max_num_workers,
        "initial_num_workers" => initial_num_workers,
        "min_num_workers" => min_num_workers,
        "aws_region" => region,
        "s3_read_write_resource" => s3_bucket_arn,
        "scaledown_time" => scaledown_time,
        "recreate" => false,
        # We need to pass in the disk capacity in # of GiB and we do this by dividing the input
        # by size of 1 GiB and then round up. Then the backend will determine how to adjust the
        # disk capacity to an allowable increment (e.g., 1200 GiB or an increment of 2400 GiB
        # for AWS FSx Lustre filesystems)
        "disk_capacity" => ceil(Int64, parse_bytes(disk_capacity) / 1.073741824e7)
    )
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

    @info "Started creating cluster named $name"

    # Send request to create cluster
    send_request_get_response(:create_cluster, cluster_config)

    if !nowait
        wait_for_cluster(name; kwargs...)
    end

    # Cache info
    get_cluster(name; kwargs...)

    return get_clusters_dict()[name]
end

function destroy_cluster(name::String; kwargs...)
    configure(; kwargs...)
    @info "Destroying cluster named $name"
    send_request_get_response(:destroy_cluster, Dict{String,Any}("cluster_name" => name))
end

function delete_cluster(name::String; kwargs...)
    configure(; kwargs...)
    @info "Deleting cluster named $name"
    send_request_get_response(
        :destroy_cluster,
        Dict{String,Any}("cluster_name" => name, "permanently_delete" => true),
    )
end

function update_cluster(name::String; nowait=false, kwargs...)
    configure(; kwargs...)
    @info "Updating cluster named $name"
    send_request_get_response(
        :update_cluster,
        Dict{String, Any}("cluster_name" => name)
    )
    if !nowait
        wait_for_cluster(name)
    end
end

function assert_cluster_is_ready(name::String; kwargs...)
    @info "Setting status of cluster named $name to running"

    # Configure
    configure(; kwargs...)

    send_request_get_response(:set_cluster_ready, Dict{String,Any}("cluster_name" => name))
end

parsestatus(status::String)::Symbol =
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
    elseif status == "unknown"
        :unknown
    else
        error("Unexpected status ", status)
    end

function _get_clusters(cluster_name::String)::Dict{String,Cluster}
    @debug "Downloading description of clusters"
    filters = Dict()
    if !isempty(cluster_name)
        filters["cluster_name"] = cluster_name
    end
    response = send_request_get_response(:describe_clusters, Dict{String,Any}("filters"=>filters))
    @show response
    clusters_dict::Dict{String,Cluster} = Dict{String,Cluster}()
    for (name::String, c::Dict{String,Any}) in response["clusters"]::Dict{String,Any}
        clusters_dict[name] = Cluster(
            name,
            parsestatus(c["status"]::String),
            haskey(c, "status_explanation") ? c["status_explanation"]::String : "",
            c["s3_read_write_resource"]::String,
            c["organization_id"]::String,
            haskey(c, "curr_cluster_instance_id") ? c["curr_cluster_instance_id"]::String : ""
        )
    end

    # Cache info
    curr_clusters_dict = get_clusters_dict()
    for (name, c) in clusters_dict
        curr_clusters_dict[name] = c
    end

    clusters_dict
end

function get_clusters(cluster_name=nothing; kwargs...)::Dict{String,Cluster}
    configure(; kwargs...)
    _get_clusters(isnothing(cluster_name) ? "" : cluster_name)
end

function get_cluster_s3_bucket_arn(cluster_name=get_cluster_name(); kwargs...)
    clusters_dict = get_clusters_dict()
    # Check if cached, sine this property is immutable
    if !haskey(clusters_dict, cluster_name)
        get_cluster(cluster_name; kwargs...)
    end
    return clusters_dict[cluster_name].s3_bucket_arn
end

get_cluster_s3_bucket_name(cluster_name=get_cluster_name(); kwargs...) =
    s3_bucket_arn_to_name(get_cluster_s3_bucket_arn(cluster_name; kwargs...))

get_cluster(name::String=get_cluster_name(); kwargs...)::Cluster = get_clusters(name; kwargs...)[name]

get_running_clusters(args...; kwargs...) = filter(entry -> entry[2].status == :running, get_clusters(args...; kwargs...))

function get_cluster_status(name::String)::Symbol
    clusters_dict = get_clusters_dict()
    clusters::Dict{String,Cluster}
    if haskey(clusters_dict, name)
        if clusters_dict[name].status == :failed
            @error clusters_dict[name].status_explanation
        end
    end
    c::Cluster = get_clusters(name)[name]
    if c.status == :failed
        @error c.status_explanation
    end
    c.status
end
get_cluster_status() = get_cluster_status(get_cluster_name())

function _wait_for_cluster(name::String)
    t::Int64 = 5
    cluster_status::Symbol = get_cluster_status(name)
    p::ProgressUnknown = ProgressUnknown("Finding status of cluster $name", enabled=false)
    while (cluster_status == :creating || cluster_status == :updating)
        if !p.enabled
            if cluster_status == :creating
                p = ProgressUnknown("Setting up cluster $name", spinner=true)
            else
                p = ProgressUnknown("Updating cluster $name", spinner=true)
            end
        end
        sleep(t)
        next!(p)
        if t < 80
            t *= 2
        end
        cluster_status = get_cluster_status(name)
    end
    if p.enabled
        finish!(p, spinner = (cluster_status == :running ? '✓' : '✗'))
    end
    if cluster_status == :running
        # @info "Cluster $name is ready"
    elseif cluster_status == :terminated
        error("Cluster $name no longer exists")
    elseif cluster_status != :creating && cluster_status != :updating
        error("Failed to set up cluster named $name")
    else
        error("Cluster $name has unexpected status: $cluster_status")
    end
end
function wait_for_cluster(kwargs...)
    configure(kwargs...)
    _wait_for_cluster(get_cluster_name())
end
function wait_for_cluster(name::String, kwargs...)
    configure(kwargs...)
    _wait_for_cluster(name)
end

function upload_to_s3(src_path; dst_name=basename(src_path), cluster_name=get_cluster_name(), kwargs...)
    configure(; kwargs...)
    bucket_name = get_cluster_s3_bucket_name(cluster_name)
    s3_dst_path = S3Path("s3://$bucket_name/$dst_name", config=get_aws_config())
    if startswith(src_path, "http://") || startswith(src_path, "https://")
        Base.download(
            src_path,
            s3_dst_path
        )
    elseif startswith(src_path, "s3://")
        cp(
            S3Path(src_path),
            s3_dst_path
        )
    else
        if startswith(src_path, "file://")
            src_path = src_path[8:end]
        end
        if isfile(src_path)
            cp(
                Path(src_path),
                s3_dst_path
            )
        else # isdir
            for f_name in readdir(src_path)
                cp(
                    Path("$src_path/$f_name"),
                    S3Path(
                        "s3://$bucket_name/$(basename(src_path))/$(f_name)",
                        config=get_aws_config()
                    )
                )
            end
        end
    end
    return dst_name
end

@specialize
