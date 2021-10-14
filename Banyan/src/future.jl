mutable struct Future <: AbstractFuture
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool

    function Future(value::Any, value_id::ValueId, mutated::Bool, stale::Bool)
        new_future = new(value, value_id, mutated, stale)

        # Create finalizer and register
        finalizer(new_future) do fut
            try
                record_request(DestroyRequest(fut.value_id))
            catch e
                # `record_request` will fail if there isn't any job to add the
                # request to. So we just continue silently.
                # @warn "Failed to destroy value $(fut.value_id) because job has stopped: $e"
            end
        end

        new_future
    end
end

isview(f::F) where F <: AbstractFuture = false