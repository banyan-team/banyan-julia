mutable struct Future <: AbstractFuture
    datatype::String
    value::Any
    value_id::ValueId
    total_memory_usage::Union{Integer,Nothing}
    mutated::Bool
    stale::Bool
    # A non-view future can still have parents and be filtered from them. These
    # `parents` and whether or not this future is filtered from them helps PT
    # constructors like `Blocked` and `Grouped`.
    parents::Vector{Future}
    filtered_from_parents::Bool
    # Specified if this is a view
    parent_tasks::Vector#{Task}

    function Future(datatype::String, value::Any, value_id::ValueId, mutated::Bool, stale::Bool, parents::Vector, filtered_from_parents::Bool)
        new_future = new(datatype, value, value_id, nothing, mutated, stale, parents, filtered_from_parents, [])

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

isview(f::AbstractFuture) = length(convert(Future, f).parent_tasks) > 0