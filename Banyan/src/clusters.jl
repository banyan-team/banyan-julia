
function create_cluster(;
    name::Union{String,Nothing} = nothing,
    instance_type::Union{String,Nothing} = "m4.4xlarge",
    max_num_workers::Union{Int,Nothing} = 2048,
    initial_num_workers::Union{Int,Nothing} = 16,
    min_num_workers::Union{Int,Nothing} = 0,
    iam_policy_arn::Union{String,Nothing} = nothing,
    s3_bucket_arn::Union{String,Nothing} = nothing,
    s3_bucket_name::Union{String,Nothing} = nothing,
    scaledown_time = 25,
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

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if haskey(clusters, name)
        if clusters[name].status == :terminated
            @warn "Cluster configuration with name $name already exists. Ignoring new configuration and re-creating cluster."
            send_request_get_response(
                :create_cluster,
                Dict("cluster_name" => name, "recreate" => true),
            )
            if !nowait
                wait_for_cluster(name)
            end
            return get_cluster(name)
        else
            error("Cluster with name $name already exists and has status $(string(clusters[name].status))")
        end
    end

    # Construct arguments
    if !isnothing(s3_bucket_name)
        s3_bucket_arn = "arn:aws:s3:::$s3_bucket_name*"
    elseif !isnothing(s3_bucket_arn)
        s3_bucket_name = last(split(s3_bucket_arn, ":"))
    end
    if isnothing(s3_bucket_arn)
        s3_bucket_arn = ""
    elseif !(s3_bucket_name in s3_list_buckets(get_aws_config()))
        error("Bucket $s3_bucket_name does not exist in connected AWS account")
    end

    # Construct cluster creation
    cluster_config = Dict(
        "cluster_name" => name,
        "instance_type" => instance_type,
        "max_num_workers" => max_num_workers,
        "initial_num_workers" => initial_num_workers,
        "min_num_workers" => min_num_workers,
        "aws_region" => get_aws_config_region(),
        "s3_read_write_resource" => s3_bucket_arn,
        "scaledown_time" => scaledown_time,
        "recreate" => false,
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

    @info "Creating cluster"

    # Send request to create cluster
    send_request_get_response(:create_cluster, cluster_config)

    if !nowait
        wait_for_cluster(name)
    end

    return Cluster(name, get_cluster_status(name), "", 0, s3_bucket_arn)
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
    status_explanation::String
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
            haskey(c, "status_explanation") ? c["status_explanation"] : "",
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

get_running_clusters(args...; kwargs...) = filter(entry -> entry[2].status == :running, get_clusters(args...; kwargs...))

function get_cluster_status(name::String=get_cluster_name(), kwargs...)
    c = get_clusters(; kwargs...)[name]
    if c.status == :failed
        @info c.status_explanation
    end
    c.status
end

function wait_for_cluster(name::String=get_cluster_name(), kwargs...)
    t = 5
    cluster_status = get_cluster_status(name; kwargs...)
    while (cluster_status == :creating || cluster_status == :updating)
        if cluster_status == :creating
            @info "Cluster $(name) is getting set up"
        else
            @info "Cluster $(name) is updating"
        end
        sleep(t)
        if t < 80
            t *= 2
        end
        cluster_status = get_cluster_status(name; kwargs...)
    end
    if cluster_status == :running
        @info "Cluster $(name) is running and ready for jobs"
    elseif cluster_status == :terminated
        @info "Cluster $(name) no longer exists"
    elseif cluster_status != :creating && cluster_status != :updating
        @info "Cluster $(name) setup has failed"
        # delete_cluster(name)
    end
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