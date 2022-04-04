mutable struct Future <: AbstractFuture
    datatype::String
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool
    total_memory_usage::Int64
end

Base.hash(f::Future) = hash(f.value_id)

is_total_memory_usage_known(f::Future) = f.total_memory_usage != -1

isview(f::AbstractFuture) = false

function create_future(datatype::String, value::Any, value_id::ValueId, mutated::Bool, stale::Bool)::Future
    new_future = Future(datatype, value, value_id, mutated, stale, -1)
    println("Creating new future with value ID $value_id and objectid(new_future)=$(objectid(new_future))")
    @show objectid(new_future)

    # Create finalizer and register
    if value_id != "04"
        finalizer(new_future) do fut
            try
                println("Recording request to destroy value with ID $(fut.value_id) with object ID $(objectid(fut))")
                @show objectid(fut)
                @show fut.value_id
                record_request(DestroyRequest(fut.value_id))
            catch e
                # `record_request` will fail if there isn't any session to add the
                # request to. So we just continue silently.
                # @warn "Failed to destroy value $(fut.value_id) because session has stopped: $e"
            end
        end
    end

    println("After creating finalizer... and objectid(new_future)=$(objectid(new_future))")

    new_future
end