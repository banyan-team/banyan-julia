mutable struct Future <: AbstractFuture
    datatype::String
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool
    total_memory_usage::Int64
end

const NOTHING_FUTURE = Future("", nothing, "", false, false, -1)
Base.isnothing(f::Future) = isempty(f.value_id)

Base.hash(f::Future) = hash(f.value_id)

is_total_memory_usage_known(f::Future) = f.total_memory_usage != -1

isview(f::AbstractFuture) = false

function create_future(datatype::String, value::Any, value_id::ValueId, mutated::Bool, stale::Bool)::Future
    new_future = Future(datatype, value, value_id, mutated, stale, -1)

    # # Create finalizer and register
    # finalizer(new_future) do fut
    #     session_id = get_sessions_dict()
    #     sessions_dict = get_sessions_dict()
    #     if !isnothing(session_id) && haskey(sessions_dict, session_id)
    #         Banyan.record_request(Banyan.DestroyRequest(fut.value_id))
    #     else
    #         # `record_request` will fail if there isn't any session to add the
    #         # request to. So we just continue silently.
    #         # @warn "Failed to destroy value $(fut.value_id) because session has stopped: $e"
    #     end
    # end

    new_future
end

value_id_getter(f) = f.value_id