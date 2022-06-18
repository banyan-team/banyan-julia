using .Parquet

Parquet_read_parquet_retry = retry(Parquet.read_parquet; delays=Banyan.exponential_backoff_1s)
Parquet_File_retry = retry(Parquet.File; delays=Banyan.exponential_backoff_1s)
nrows_retry = retry(nrows; delays=Banyan.exponential_backoff_1s)
DataFrames_DataFrame_retry = retry(DataFrames.DataFrame; delays=Banyan.exponential_backoff_1s)

# locations.jl

has_separate_metadata(::Val{:parquet}) = true
get_metadata(::Val{:parquet}, p)::Int64 =
    try
        nrows_retry(Parquet_File_retry(p))
    catch
        # File does not exist
        0
    end
get_sample(::Val{:parquet}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if (sample_rate != 1.0 && isempty(rand_indices))
        DataFrames.DataFrame()
    else
        try
            get_sample_from_data(DataFrames_DataFrame_retry(Parquet_read_parquet_retry(p; rows=1:len), copycols=false), sample_rate, rand_indices)
        catch
            # File does not exist
            DataFrames.DataFrame()
        end
    end
end
get_sample_and_metadata(::Val{:parquet}, p, sample_rate) =
    try
        let sample_df = DataFrames_DataFrame_retry(Parquet_File_retry(p), copycols=false)
            num_rows = nrow(sample_df)
            get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
        end
    catch
        # File does not exist
        DataFrames.DataFrame(), 0
    end

# pfs.jl

file_ending(::Val{:parquet}) = "parquet"

function read_file(::Val{:parquet}, path, rowrange, readrange, filerowrange)
    try
        let f = Parquet_read_parquet_retry(
            path;
            rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
        )
            DataFrames_DataFrame_retry(f, copycols=false)
        end
    catch
        # File does not exist
        !startswith(path, "efs/s3/") || error("Path \"$path\" should not start with \"s3/\"")
        DataFrames.DataFrame()
    end
end
read_file(::Val{:parquet}, path) =
    try
        let f = Parquet_read_parquet_retry(path)
            DataFrames_DataFrame_retry(f, copycols=false)
        end
    catch
        !startswith(path, "efs/s3/") || error("Path \"$path\" should not start with \"s3/\"")
        DataFrames.DataFrame()
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