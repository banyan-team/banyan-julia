const AnyDataFrame = Union{
    DataFrames.DataFrame,
    SubDataFrame{DataFrames.DataFrame, DataFrames.Index, Base.Vector{Int64}},
    SubDataFrame{DataFrames.DataFrame, DataFrames.Index, UnitRange{Int64}}
}

Banyan.split_on_executor(
    src::DataFrames.DataFrame,
    d::Int64,
    i::UnitRange{Int64}
)::SubDataFrame{DataFrames.DataFrame, DataFrames.Index, UnitRange{Int64}} = @view src[i, :]
# Banyan.split_on_executor(src::DataFrames.GroupedDataFrame, d::Int64, i::UnitRange{Int64}) = nothing

# In case we are trying to `Distribute` a grouped data frame,
# we can't do that so we will simply return nothing so that the groupby
# partitioned computation will redo the groupby.

Banyan.split_on_executor(
    src::Union{Nothing,DataFrames.GroupedDataFrame},
    dim::Int64,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
) = nothing

# If this is a dataframe then we ignore the grouping key
function Banyan.merge_on_executor(
    obj::Base.Vector{DF},
    key
) where DF <: AnyDataFrame
    res = if length(obj) == 1
        obj[1]
    else
        vcat(obj...)
    end
    res
end

# Banyan.merge_on_executor(obj::Base.Vector{DataFrames.GroupedDataFrame{<:AbstractDataFrame}}; key = nothing) = nothing
# function Banyan.merge_on_executor(obj::Base.Vector{T}; key = nothing)::T where {T} first(obj) end

function Banyan.sync_across(df::DataFrames.DataFrame; comm=MPI.COMM_WORLD)
    # An optimized version of sync_across that syncs data frames across workers
    is_main = is_main_worker(comm)
    count = Ref{Cint}()
    if is_main
        io = IOBuffer()
        Arrow.write(io, df)
        buf = MPI.Buffer(view(io.data, 1:io.size))
        count[] = length(buf.data)
    end
    MPI.Bcast!(count, 0, comm)
    if !is_main
        buf = MPI.Buffer(Base.Array{UInt8}(undef, count[]))
    end
    MPI.Bcast!(buf, 0, comm)
    DataFrames.DataFrame(Arrow.Table(IOBuffer(view(buf.data, 1:buf.count))))
end