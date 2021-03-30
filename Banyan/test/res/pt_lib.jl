using MPI

function split_nothing(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)::Nothing
    nothing
end

function merge_nothing(
    src,
    dst,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    src
end

get_worker_idx(comm::MPI.Comm) = MPI.Comm_rank(comm) + 1
get_nworkers(comm::MPI.Comm) = MPI.Comm_size(comm)

split_len(src_len::Int64, idx::Int64, npartitions::Int64) =
    if npartitions > 1
        dst_len = Int64(cld(src_len, npartitions))
        dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
        dst_end = min(idx * dst_len, src_len)
        dst_start:dst_end
    else
        1:src_len
    end

split_len(src_len, comm::MPI.Comm) =
    split_len(src_len, worker_idx(comm), nworkers(comm))

split_len(
    src_len,
    batch_idx::Int64,
    nbatches::Int64,
    worker_idx::Int64,
    nworkers::Int64,
) = split_len(
    src_len,
    (worker_idx - 1) * nworkers + batch_idx,
    nworkers * nbatches,
)

split_len(src_len, batch_idx::Int64, nbatches::Int64, comm::MPI.Comm) =
    split_len(src_len, batch_idx, nbatches, worker_idx(comm), nworkers(comm))

split_array(src::Array, dim::Int8, args...) =
    if npartitions > 1
        src_len = dst_start, dst_end = split_len(size(src, dim), arg...)
        selectdim(src, dim, dst_start:dst_end)
    else
        src
    end

isoverlapping(a::AbstractRange, b::AbstractRAnge) =
    a.start ≤ b.stop && b.start ≤ a.stop

function ReadCSV(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    nrows = loc_parameters["nrow"]
    rowrange = split_len(nrows, batch_idx, nbatches, comm)

    rowsscanned = 0
    if haskey(loc_parameters, "files")
        dfs = DataFrame()
        for file in loc_parameters["files"]
            newrowsscanned = rowsscanned + file["nrows"]
            filerowrange = (rowsscanned+1):newrowsscanned
            if isoverlapping(filerowrange, rowrange)
                readrange = filerowrange
                if rowrange.start > filerowrange.start
                    readrange.start = rowrange.start
                end
                if rowrange.stop < filerowrange.stop
                    readrange.stop = rowrange.stop
                end
                push!(dfs, DataFrame(
                    if haskey(file, "s3_bucket")
                        CSV.File(
                            s3_get(file["s3_bucket"], file["s3_key"]),
                            skipto = readrange.start - filerowrange.start + 1,
                            footerskip = filerowrange.stop - readrange.stop,
                        ) |> Arrow.Table
                    elseif haskey(file, "path")
                        CSV.File(
                            file["path"],
                            skipto = readrange.start - filerowrange.start + 1,
                            footerskip = filerowrange.stop - readrange.stop,
                        ) |> Arrow.Table
                    else
                        error("Expected file with s3_bucket or local path")
                    end,
                ))
            end
            rowsscanned = newrowsscanned
        end
        vcat(dfs...)
    elseif haskey(loc_parameters, "url")
        error("Reading CSV file from URL is not currently supported")
    else
        error("Expected either files or a URL to download CSV dataset from")
    end
end

function ReadCachedCSV(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    name = loc_parameters["name"]
    if isdir(name)
        ReadCSV(src, params, batch_idx, batches, comm, "CSV", Dict("files" => [
            Dict("path" => joinpath(name, f))
            for f in readdir(name)
        ]))
    else
        nothing
    end
end

function WriteCSV(
    src,
    dst,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    if haskey(loc_parameters, "s3_bucket")
        s3_bucket = loc_parameters["s3_bucket"]
    else
        error("Expected S3 bucket to write CSV to")
    end
end