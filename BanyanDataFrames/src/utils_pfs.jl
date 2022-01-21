Banyan.split_on_executor(src::AbstractDataFrame, d::Integer, i) = @view src[i, :]
Banyan.split_on_executor(src::GroupedDataFrame, d::Integer, i) = nothing

# In case we are trying to `Distribute` a grouped data frame,
# we can't do that so we will simply return nothing so that the groupby
# partitioned computation will redo the groupby.

Banyan.split_on_executor(
    src::Union{Nothing,GroupedDataFrame},
    dim::Integer,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
) = nothing

# If this is a dataframe then we ignore the grouping key
function merge_on_executor(obj::Vararg{AbstractDataFrame,M}; key = nothing) where {M}
    if length(obj) == 1
        obj[1]
    else
        vcat(obj...)
    end
end

merge_on_executor(obj::Vararg{GroupedDataFrame,M}; key = nothing) where {M} = nothing
merge_on_executor(obj::Vararg{T,M}; key = nothing) where {T,M} = first(obj)