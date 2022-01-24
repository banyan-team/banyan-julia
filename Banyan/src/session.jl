mutable struct Session
    id::SessionId
    resource_id::ResourceId
    nworkers::Int32
    sample_rate::Int32
    locations::Dict{ValueId,Location}
    pending_requests::Vector{Request}
    # This is a `WeakKeyDict` so that futures can be GC-ed as long as all
    # references elsewhere are gone.
    futures_on_client::WeakKeyDict{ValueId,Future}
    cluster_name::String
    # To know the current status of the jsessionob, check if it is in the global
    # `sessions`. If it is, it's running. If it's not it either failed or
    # completed (`end_session` being called doesn't necessarily mean that the
    # session failed).

    # This struct just stores local state for the session.
    function Session(cluster_name::String, session_id::SessionId, resource_id::ResourceId, nworkers::Integer, sample_rate::Integer)::Session
        new(session_id, resource_id, nworkers, sample_rate, Dict(), [], Dict(), cluster_name)
    end
end


