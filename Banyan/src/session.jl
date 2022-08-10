mutable struct Session
    id::SessionId
    resource_id::ResourceId
    nworkers::Int64
    locations::Dict{ValueId,Location}
    pending_requests::Vector{Request}
    # This is a `WeakKeyDict` so that futures can be GC-ed as long as all
    # references elsewhere are gone.
    futures_on_client::Dict{ValueId,Future}
    cluster_name::String
    worker_memory_used::Int64
    # To know the current status of the jsessionob, check if it is in the global
    # `sessions`. If it is, it's running. If it's not it either failed or
    # completed (`end_session` being called doesn't necessarily mean that the
    # session failed).
    organization_id::String
    cluster_instance_id::String
    not_using_modules::Vector{String}
    loaded_packages::Set{String}
    is_cluster_ready::Bool
    is_session_ready::Bool
    scatter_queue_url::String
    gather_queue_url::String
    execution_queue_url::String

    # This struct just stores local state for the session.
    function Session(
        cluster_name::String,
        session_id::SessionId,
        resource_id::ResourceId,
        nworkers::Int64,
        organization_id::String = "",
        cluster_instance_id::String = "",
        not_using_modules::Vector{String} = NOT_USING_MODULES,
        is_cluster_ready::Bool = false,
        is_session_ready::Bool = false;
        scatter_queue_url::String = "",
        gather_queue_url::String = "",
        execution_queue_url::String = ""
    )::Session
        new(
            session_id,
            resource_id,
            nworkers,
            Dict{ValueId,Location}(),
            [],
            Dict{ValueId,Future}(),
            cluster_name,
            0,
            organization_id,
            cluster_instance_id,
            not_using_modules,
            Set{String}(),
            is_cluster_ready,
            is_session_ready,
            scatter_queue_url,
            gather_queue_url,
            execution_queue_url
        )
    end
end

function sampling_configs_to_jl(sampling_configs::Dict{LocationPath,SamplingConfig})
    res = Tuple{Tuple{String,String,String},Tuple{Int64,Bool,Int64,Bool,Bool}}[]
    for (l::LocationPath, s::SamplingConfig) in sampling_configs
        push!(
            res,
            (
                (l.original_path, l.format_name, l.format_version),
                (s.rate, s.always_exact, s.max_num_bytes_exact, s.force_new_sample_rate, s.assume_shuffled),
            ),
        )
    end
    res
end

function sampling_configs_from_jl(sampling_configs)
    res = Dict{LocationPath,SamplingConfig}()
    for (l, s) in sampling_configs
        res[LocationPath(l[1], l[2], l[3])] = SamplingConfig(s[1], s[2], s[3], s[4], s[5])
    end
    res
end