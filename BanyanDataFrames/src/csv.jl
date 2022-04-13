using .CSV

# locations.jl

get_csv_chunks(localfilepathp::String)::Any = 
    try
        CSV.Chunks(localfilepathp)
    catch e
        # An ArgumentError may get thrown if the file cannot be
        # read in with the multi-threaded chunked iterator for
        # some reason. See
        # https://github.com/JuliaData/CSV.jl/blob/main/src/context.jl#L583-L641
        # for possible reasons for `ctx.threaded` in CSV.jl
        # code to be false.
        if isa(e, ArgumentError)
            [CSV.File(localfilepathp)]
        else
            throw(e)
        end
    end

read_chunk(localfilepathp::String, ::Val{:csv}) = get_csv_chunks(localfilepathp)

function get_nrow(localfilepathp::String, ::Val{:csv})
    num_rows = 0
    for _ in CSV.Rows(localfilepathp)
        num_rows += 1
    end
    num_rows
end

# pfs.jl

function read_csv_file(path, header, rowrange, readrange, filerowrange, dfs)
    f = CSV.File(
        path,
        header = header,
        skipto = header + readrange.start - filerowrange.start + 1,
        footerskip = filerowrange.stop - readrange.stop,
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

ReadBlockCSV = ReadBlockHelper(read_csv_file, ".csv")
ReadGroupHelperCSV = ReadGroupHelper(ReadBlockCSV, ShuffleDataFrame)
ReadGroupCSV = ReadGroup(ReadGroupHelperCSV)

write_csv_file(part::DataFrames.DataFrame, path, sortableidx, nrows) = CSV.write(
    joinpath(path, "part$sortableidx" * "_nrows=$nrows.csv"),
    part
)

WriteCSV = WriteHelper(write_csv_file)

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
    ReadBlockCSV(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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