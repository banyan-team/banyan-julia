mutable struct Future <: AbstractFuture
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool

    function Future(value::Any, value_id::ValueId, mutated::Bool, stale::Bool)
        new_future = new(value, value_id, mutated, stale)

        # Create finalizer and register
        finalizer(new_future) do fut
            record_request(DestroyRequest(fut.value_id))
        end

        new_future
    end
end