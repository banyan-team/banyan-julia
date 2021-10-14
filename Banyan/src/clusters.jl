
function create_cluster(;
    name::Union{String,Nothing} = nothing,
    instance_type::Union{String,Nothing} = "m4.4xlarge",
    max_num_nodes::Union{Int,Nothing} = 8,
    initial_num_workers::Union{Int,Nothing} = 0,
    min_num_workers::Union{Int,Nothing} = 0,
    iam_policy_arn::Union{String,Nothing} = nothing,
    s3_bucket_arn::Union{String,Nothing} = nothing,
    s3_bucket_name::Union{String,Nothing} = nothing,
    scaledown_time = 25,
    ec2_key_pair_name = nothing,
    vpc_id = nothing,
    subnet_id = nothing,
    kwargs...,
)
    clusters = get_clusters(; kwargs...)
    if isnothing(name)
        name = "Cluster " * string(length(clusters) + 1)
    end

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if haskey(clusters, name)
        if clusters[name][status] == "terminated"
            @warn "Cluster configuration with name $name already exists. Ignoring new configuration and re-creating cluster."
            send_request_get_response(
                :create_cluster,
                Dict("cluster_name" => name, "recreate" => true),
            )
            return
        else
            error("Cluster with name $name already exists")
        end
    end

    # Construct arguments

    # Configure using parameters
    c = configure(; kwargs...)

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
        "initial_num_workers" => initial_num_workers,
        "min_num_workers" => min_num_workers,
        "aws_region" => get_aws_config_region(),
        "s3_read_write_resource" => s3_bucket_arn,
        "scaledown_time" => scaledown_time,
        "recreate" => false,
    )
    if !isnothing(ec2_key_pair_name)
        cluster_config["ec2_key_pair"] = ec2_key_pair_name
    else haskey(c["aws", "ec2_key_pair_name"])
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

    @info "Creating cluster"

    # Send request to create cluster
    send_request_get_response(:create_cluster, cluster_config)

    return Cluster(name, :creating, 0, s3_bucket_arn)
end

function destroy_cluster(name::String; kwargs...)
    configure(; kwargs...)
    @debug "Destroying cluster"
    send_request_get_response(:destroy_cluster, Dict{String,Any}("cluster_name" => name))
end

function delete_cluster(name::String; kwargs...)
    configure(; kwargs...)
    @debug "Deleting cluster"
    send_request_get_response(
        :destroy_cluster,
        Dict{String,Any}("cluster_name" => name, "permanently_delete" => true),
    )
end

function update_cluster(name::String; kwargs...)
    configure(; kwargs...)
    @debug "Updating cluster"
    send_request_get_response(
        :update_cluster,
        Dict{String, Any}("cluster_name" => name)
    )
end

function assert_cluster_is_ready(name::String; kwargs...)
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

function get_cluster_s3_bucket_name(cluster_name=get_cluster_name(); kwargs...)
    configure(; kwargs...)
    cluster = get_cluster(cluster_name)
    return s3_bucket_arn_to_name(cluster.s3_bucket_arn)
end

get_cluster(name::String=get_cluster_name(), kwargs...) = get_clusters(; kwargs...)[name]

get_cluster_status(name::String=get_cluster_name(), kwargs...) = get_clusters(; kwargs...)[name].status

get_running_clusters(args...; kwargs...) = filter(entry -> entry[2].status == :running, get_clusters(args...; kwargs...))