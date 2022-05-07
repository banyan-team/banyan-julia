using .CSV

# locations.jl

has_separate_metadata(::Val{:csv}) = false
function get_metadata(::Val{:csv}, p)
    # num_rows = 0
    # for _ in CSV.Rows(p)
    #     num_rows += 1
    # end
    # num_rows
    # This should never be called because has_separate_metadata = false and we don't
    # want to unnecessarily compile CSV.Rows
    nrow(CSV.read(p, DataFrames.DataFrame))
end
get_sample(::Val{:csv}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if isempty(rand_indices)
        DataFrames.DataFrame()
    else
        get_sample_from_data(CSV.read(p, DataFrames.DataFrame), sample_rate, rand_indices)
    end
end
get_sample_and_metadata(::Val{:csv}, p, sample_rate) =
    let sample_df = CSV.read(p, DataFrames.DataFrame)
        num_rows = nrow(sample_df)
        get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
    end

# get_csv_chunks(localfilepathp::String)::Any = 
#     try
#         CSV.Chunks(localfilepathp)
#     catch e
#         # An ArgumentError may get thrown if the file cannot be
#         # read in with the multi-threaded chunked iterator for
#         # some reason. See
#         # https://github.com/JuliaData/CSV.jl/blob/main/src/context.jl#L583-L641
#         # for possible reasons for `ctx.threaded` in CSV.jl
#         # code to be false.
#         if isa(e, ArgumentError)
#             [CSV.File(localfilepathp)]
#         else
#             throw(e)
#         end
#     end

# read_chunk(localfilepathp::String, ::Val{:csv}) = get_csv_chunks(localfilepathp)

# function get_nrow(localfilepathp::String, ::Val{:csv})
#     num_rows = 0
#     for _ in CSV.Rows(localfilepathp)
#         num_rows += 1
#     end
#     num_rows
# end 

# pfs.jl

file_ending(::Val{:csv}) = "csv"

function read_file(::Val{:csv}, path, header, rowrange, readrange, filerowrange, dfs)
    @time begin
    et = @elapsed begin
    CSV.read(
        path,
        DataFrames.DataFrame
    )
    end
    println("Time on worker_idx=$(get_worker_idx()) for first CSV.read in read_file: $et seconds")
    end
    push!(
        dfs,
        CSV.read(
            path,
            DataFrames.DataFrame;
            header = header,
            skipto = header + readrange.start - filerowrange.start + 1,
            footerskip = filerowrange.stop - readrange.stop,
        )
    )
end

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
        ReadBlockCSV(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    else
        DataFrames.DataFrame()
    end
    sync_across(part, com=comm)
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