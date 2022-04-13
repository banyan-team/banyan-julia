using .Parquet

# locations.jl

read_chunk(localfilepathp::String, ::Val{:parquet}) = Tables.partitions(Parquet.read_parquet(localfilepathp))

get_nrow(localfilepathp::String, ::Val{:parquet}) = nrows(Parquet.File(localfilepathp))

# pfs.jl

function read_parquet_file(path, header, rowrange, readrange, filerowrange, dfs)
    f = Parquet.read_parquet(
        path,
        rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

ReadBlockParquet = ReadBlockHelper(read_parquet_file, ".parquet")
ReadGroupHelperParquet = ReadGroupHelper(ReadBlockParquet, ShuffleDataFrame)
ReadGroupParquet = ReadGroup(ReadGroupHelperParquet)

write_parquet_file(part::DataFrames.DataFrame, path, sortableidx, nrows) = if nrows > 0
    Parquet.write_parquet(joinpath(path, "part$sortableidx" * "_nrows=$nrows.parquet"), part)
end

WriteParquet = WriteHelper(write_parquet_file)

CopyFromParquet(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)::DataFrames.DataFrame = begin
    params["key"] = 1
    ReadBlockParquet(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

function CopyToParquet(
    src,
    part::Union{DataFrames.AbstractDataFrame,Empty},
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
        params["key"] = 1
        WriteParquet(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
end

# df.jl

read_parquet(p; kwargs...) = read_table(p; kwargs...)
write_parquet(A, p; kwargs...) = write_table(A, p; kwargs...)