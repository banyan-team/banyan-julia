# locations.jl

has_separate_metadata(::Val{:arrow}) = true
get_metadata(::Val{:arrow}, p) = Tables.rowcount(Arrow.Table(localfilepathp))
get_sample(::Val{:arrow}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if isempty(rand_indices)
        DataFrames.DataFrame()
    else
        get_sample_from_data(DataFrames.DataFrame(Arrow.Table(p); copycols=false), sample_rate, rand_indices)
    end
end
get_sample_and_metadata(::Val{:arrow}, p, sample_rate) =
    let sample_df = DataFrames.DataFrame(Arrow.Table(p); copycols=false)
        num_rows = nrow(sample_df)
        get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
    end

# read_chunk(localfilepathp::String, ::Val{:arrow}) = Arrow.Stream(localfilepathp)

# get_nrow(localfilepathp::String, ::Val{:arrow}) = Tables.rowcount(Arrow.Table(localfilepathp))

# pfs.jl

file_ending(::Val{:arrow}) = "arrow"

function read_file(::Val{:arrow}, path, header, rowrange, readrange, filerowrange, dfs)
    rbrowrange = filerowrange.start:(filerowrange.start-1)
    for tbl in Arrow.Stream(path)
        rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+Tables.rowcount(tbl))
        if Banyan.isoverlapping(rbrowrange, rowrange)
            readrange =
                max(rowrange.start, rbrowrange.start):min(
                    rowrange.stop,
                    rbrowrange.stop,
                )
            df = let unfiltered = DataFrames.DataFrame(tbl; copycols=false)
                unfiltered[
                    (readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1),
                    :,
                ]
            end
            push!(dfs, df)
        end
    end
end

ReadBlockArrow = ReadBlockHelper(Val(:arrow))
ReadGroupHelperArrow = ReadGroupHelper(ReadBlockArrow, ShuffleDataFrame)
ReadGroupArrow = ReadGroup(ReadGroupHelperArrow)

write_file(::Val{:arrow}, part::DataFrames.DataFrame, path, nrows) = Arrow.write(path, part)

WriteArrow = WriteHelper(Val(:arrow))

CopyFromArrow(
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
        ReadBlockArrow(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    else
        DataFrames.DataFrame()
    end
    sync_across(part, comm=comm)
end

function CopyToArrow(
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
        WriteArrow(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
end

# df.jl

read_arrow(p; kwargs...) = read_table(p; kwargs...)
write_arrow(A, p; kwargs...) = write_table(A, p; kwargs...)