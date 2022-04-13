# locations.jl

read_chunk(localfilepathp::String, ::Val{:arrow}) = Arrow.Stream(localfilepathp)

get_nrow(localfilepathp::String, ::Val{:arrow}) = Tables.rowcount(Arrow.Table(localfilepathp))

# pfs.jl

function read_arrow_file(path, header, rowrange, readrange, filerowrange, dfs)
    rbrowrange = filerowrange.start:(filerowrange.start-1)
    for tbl in Arrow.Stream(path)
        rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+Tables.rowcount(tbl))
        if Banyan.isoverlapping(rbrowrange, rowrange)
            readrange =
                max(rowrange.start, rbrowrange.start):min(
                    rowrange.stop,
                    rbrowrange.stop,
                )
            df = let unfiltered = DataFrames.DataFrame(tbl, copycols=false)
                unfiltered[
                    (readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1),
                    :,
                ]
            end
            push!(dfs, df)
        end
    end
end

ReadBlockArrow = ReadBlockHelper(read_arrow_file, ".arrow")
ReadGroupHelperArrow = ReadGroupHelper(ReadBlockArrow, ShuffleDataFrame)
ReadGroupArrow = ReadGroup(ReadGroupHelperArrow)

write_arrow_file(part::DataFrames.DataFrame, path, sortableidx, nrows) = Arrow.write(
    joinpath(path, "part$sortableidx" * "_nrows=$nrows.arrow"),
    part
)

WriteArrow = WriteHelper(write_arrow_file)

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
    ReadBlockArrow(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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