using .Parquet

# locations.jl

has_separate_metadata(::Val{:parquet}) = true
get_metadata(::Val{:parquet}, p)::Int64 = isfile(p) ? nrows(Parquet.File(p)) : 0
get_sample(::Val{:parquet}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if (sample_rate != 1.0 && isempty(rand_indices)) || !isfile(p)
        DataFrames.DataFrame()
    else
        println("In get_sample for Parquet with isfile(p)=$(isfile(p))")
        get_sample_from_data(DataFrames.DataFrame(Parquet.read_parquet(p; rows=1:len), copycols=false), sample_rate, rand_indices)
    end
end
get_sample_and_metadata(::Val{:parquet}, p, sample_rate) = if isfile(p)
    let sample_df = DataFrames.DataFrame(Parquet.File(p), copycols=false)
        num_rows = nrow(sample_df)
        get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
    end
else
    DataFrames.DataFrame(), 0
end

# read_chunk(localfilepathp::String, ::Val{:parquet}) = Tables.partitions(Parquet.read_parquet(localfilepathp))

# get_nrow(localfilepathp::String, ::Val{:parquet}) = nrows(Parquet.File(localfilepathp))

# pfs.jl

file_ending(::Val{:parquet}) = "parquet"

function read_file(::Val{:parquet}, path, rowrange, readrange, filerowrange, dfs)
    push!(
        dfs,
        if isfile(path)
            let f = Parquet.read_parquet(
                path;
                rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
            )
                DataFrames.DataFrame(f, copycols=false)
            end
        else
            !startswith(path, "efs/s3/") || error("Path \"$path\" should not start with \"s3/\"")
            DataFrames.DataFrame()
        end
    )
end

ReadBlockParquet = ReadBlockHelper(Val(:parquet))
ReadGroupHelperParquet = ReadGroupHelper(ReadBlockParquet, ShuffleDataFrame)
ReadGroupParquet = ReadGroup(ReadGroupHelperParquet)

write_file(::Val{:parquet}, part::DataFrames.DataFrame, path, nrows) =
    if nrows > 0
        Parquet.write_parquet(path, part)
    end

WriteParquet = WriteHelper(Val(:parquet))

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
    part::DataFrames.DataFrame = if is_main_worker(comm)
        ReadBlockParquet(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    else
        DataFrames.DataFrame()
    end
    sync_across(part, comm=comm)
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