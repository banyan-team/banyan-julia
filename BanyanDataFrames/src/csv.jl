using .CSV

CSV_read_retry = retry(CSV.read; delays=Base.ExponentialBackOff(; n=5))

# locations.jl

has_separate_metadata(::Val{:csv}) = false
function get_metadata(::Val{:csv}, p)::Int64
    # num_rows = 0
    # for _ in CSV.Rows(p)
    #     num_rows += 1
    # end
    # num_rows
    # This should never be called because has_separate_metadata = false and we don't
    # want to unnecessarily compile CSV.Rows
    nrow(CSV_read_retry(p, DataFrames.DataFrame; header=1, skipto=2, footerskip=0))
end
get_sample(::Val{:csv}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if sample_rate != 1.0 && isempty(rand_indices)
        DataFrames.DataFrame()
    else
        get_sample_from_data(CSV_read_retry(p, DataFrames.DataFrame; header=1, skipto=2, footerskip=0), sample_rate, rand_indices)
    end
end
get_sample_and_metadata(::Val{:csv}, p, sample_rate) =
    let sample_df = CSV_read_retry(p, DataFrames.DataFrame; header=1, skipto=2, footerskip=0)
        num_rows = nrow(sample_df)
        get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
    end

# pfs.jl

file_ending(::Val{:csv}) = "csv"

function read_file(::Val{:csv}, path, rowrange, readrange, filerowrange, dfs)
    CSV_read_retry(
        path,
        DataFrames.DataFrame;
        header = 1,
        # TODO: Ensure this is okay
        skipto = 1 + readrange.start - filerowrange.start + 1,
        footerskip = filerowrange.stop - readrange.stop,
    )
end
read_file(::Val{:csv}, path) = CSV_read_retry(path, DataFrames.DataFrame)

ReadBlockCSV = ReadBlockHelper(Val(:csv))
ReadGroupHelperCSV = ReadGroupHelper(ReadBlockCSV, ShuffleDataFrame)
ReadGroupCSV = ReadGroup(ReadGroupHelperCSV)

write_file(::Val{:csv}, part::DataFrames.DataFrame, path, nrows) = CSV.write(path, part)

WriteCSV = WriteHelper(Val(:csv))

CopyFromCSV(
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
        part = ReadBlockCSV(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        part
    else
        DataFrames.DataFrame()
    end
    res = sync_across(part, comm=comm)
    res
end

function CopyToCSV(
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
        WriteCSV(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
end

# df.jl

read_csv(p; kwargs...) = read_table(p; kwargs...)
write_csv(A, p; kwargs...) = write_table(A, p; kwargs...)