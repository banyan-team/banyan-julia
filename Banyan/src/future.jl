mutable struct Future <: AbstractFuture
    datatype::String
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool
    total_memory_usage::Int64

    function Future(datatype::String, value::Any, value_id::ValueId, mutated::Bool, stale::Bool)
        new_future = new(datatype, value, value_id, mutated, stale, -1)

        # Create finalizer and register
        finalizer(new_future) do fut
            try
                record_request(DestroyRequest(fut.value_id))
            catch e
                # `record_request` will fail if there isn't any session to add the
                # request to. So we just continue silently.
                # @warn "Failed to destroy value $(fut.value_id) because session has stopped: $e"
            end
        end

        new_future
    end
end

Base.hash(f::Future) = hash(f.value_id)

is_total_memory_usage_known(f::Future) = f.total_memory_usage != -1

isview(f::AbstractFuture) = false