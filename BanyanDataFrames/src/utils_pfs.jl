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
        Arrow.write(io, df, compress=:zstd)
        buf = MPI.Buffer(view(io.data, 1:io.size))
        count[] = length(buf.data)
    end
    MPI.Bcast!(count, 0, comm)
    if !is_main
        buf = MPI.Buffer(Base.Vector{UInt8}(undef, count[]))
    end
    MPI.Bcast!(buf, 0, comm)
    DataFrames.DataFrame(Arrow.Table(IOBuffer(view(buf.data, 1:buf.count))))
end

function get_variable_sized_blob(whole_blob::Base.Vector{UInt8})
    blob_length = reinterpret(Int64, whole_blob[1:8])[1]
    whole_blob[9:(8+blob_length)]
end

function make_reducev_op(op)
    (a, b) -> begin
        a_df = a |> Base.collect |> get_variable_sized_blob |> IOBuffer |> Arrow.Table |> DataFrames.DataFrame
        b_df = b |> Base.collect |> get_variable_sized_blob |> IOBuffer |> Arrow.Table |> DataFrames.DataFrame
        res_df = op(a_df, b_df)
        res_io = IOBuffer(sizehint=length(a))
        write(res_io, reinterpret(UInt8, Int64[1]))
        Arrow.write(res_io, res_df)
        res_blob_length_blob = reinterpret(UInt8, [res_io.size - 8])
        if length(res_blob_length_blob) != 8
            error("Data frame being reduced is so large that its size cannot be represented with 8 bytes")
        end
        res_io.data[1:8] = res_blob_length_blob
        res = Tuple(res_io.data[1:length(a)])
        @show length(res)
        @show length(res_io.data)
        res
    end
end

function Banyan.reduce_across(op::Function, df::DataFrames.AbstractDataFrame; to_worker_idx=1, comm=MPI.COMM_WORLD, sync_across=false)
    # An optimized version of sync_across that syncs data frames across workers
    io = IOBuffer()
    Arrow.write(io, df, compress=:zstd)
    blob_length = MPI.Allreduce(io.size, +, comm)
    blob_length_blob = reinterpret(UInt8, [blob_length])
    if length(blob_length_blob) != 8
        error("Data frame being reduced is so large that its size cannot be represented with 8 bytes")
    end
    sized_blob_length = blob_length + 8
    reducable_blob = Base.Vector{UInt8}(undef, sized_blob_length)
    reducable_blob[1:8] = blob_length_blob
    reducable_blob[9:(8+io.size)] = view(io.data, 1:io.size)
    reduced_blob = Base.Vector{UInt8}(undef, sized_blob_length)
    reducing_dtype = MPI.Datatype(NTuple{sized_blob_length, UInt8})
    reducable_buf = MPI.RBuffer(reducable_blob, reduced_blob, 1, reducing_dtype)
    reducing_op = MPI.Op(make_reducev_op(op), Base.NTuple{sized_blob_length, UInt8})
    @show reducable_blob
    @show reduced_blob
    @show reducing_dtype
    @show blob_length
    @show Base.NTuple{sized_blob_length, UInt8}
    @show reducing_op
    if sync_across
        MPI.Allreduce!(reducable_buf, reducing_op, comm)
    else
        MPI.Reduce!(reducable_buf, reducing_op, to_worker_idx-1, comm)
    end
    if sync_across || get_worker_idx(comm) == to_worker_idx
        get_variable_sized_blob(reduced_blob) |> IOBuffer |> Arrow.Table |> DataFrames.DataFrame
    else
        nothing
    end
end

Banyan.reduce_and_sync_across(op::Function, df::DataFrames.AbstractDataFrame; comm=MPI.COMM_WORLD) =
    reduce_across(op, df; comm=comm, sync_across=true)