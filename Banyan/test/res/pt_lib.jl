using Serialization

using MPI

function split_nothing(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)::Nothing
    nothing
end

function merge_nothing(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    src
end

isa_df(obj) = @isdefined(AbstractDataFrame) && obj isa AbstractDataFrame
isa_array(obj) = obj isa AbstractArray

get_worker_idx(comm::MPI.Comm) = MPI.Comm_rank(comm) + 1
get_nworkers(comm::MPI.Comm) = MPI.Comm_size(comm)

get_partition_idx(batch_idx, nbatches, comm::MPI.Comm) =
    (get_worker_idx(comm) - 1) * nbatches + batch_idx

get_npartitions(nbatches, comm::MPI.Comm) =
    nbatches * get_nworkers(comm)

split_len(src_len::Integer, idx::Integer, npartitions::Integer) =
    if npartitions > 1
        dst_len = Int64(cld(src_len, npartitions))
        dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
        dst_end = min(idx * dst_len, src_len)
        dst_start:dst_end
    else
        1:src_len
    end

split_len(src_len, batch_idx::Integer, nbatches::Integer, comm::MPI.Comm) =
    begin
        split_len(
            src_len,
            get_partition_idx(batch_idx, nbatches, comm),
            get_npartitions(nbatches, comm)
        )
    end

split_on_executor(src, d::Integer, i) =
    if isa_df(src)
        @view src[i, :]
    elseif isa_array(src)
        selectdim(src, d, i)
    else
        error("Expected split across either dimension of an AbstractArray or rows of an AbstractDataFrame")
    end

split_on_executor(src, dim::Integer, batch_idx::Integer, nbatches::Integer, comm::MPI.Comm) =
    begin
        npartitions = get_npartitions(nbatches, comm)
        if npartitions > 1
            split_on_executor(
                src,
                dim,
                split_len(
                    size(src, dim),
                    get_partition_idx(batch_idx, nbatches, comm),
                    npartitions
                )
            )
        else
            src
        end
    end

isoverlapping(a::AbstractRange, b::AbstractRange) =
    a.start ≤ b.stop && b.start ≤ a.stop

function ReadCSV(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    nrows = loc_parameters["nrows"]
    rowrange = split_len(nrows, batch_idx, nbatches, comm)

    rowsscanned = 0
    if haskey(loc_parameters, "files")
        dfs = []
        for file in loc_parameters["files"]
            newrowsscanned = rowsscanned + file["nrows"]
            filerowrange = (rowsscanned+1):newrowsscanned
            if isoverlapping(filerowrange, rowrange)
                # TODO: Fix loading data into Arrow.Table
                # TODO: Fix issue wit dim field
                readrange = max(rowrange.start, filerowrange.start):min(rowrange.stop, filerowrange.stop)
                header = 1
                push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(
                    if haskey(file, "s3_bucket")
                        CSV.File(
                            s3_get(file["s3_bucket"], file["s3_key"], raw=true),
                            header=header,
                            skipto = header + readrange.start - filerowrange.start + 1,
                            footerskip = filerowrange.stop - readrange.stop,
                        )
                    elseif haskey(file, "path")
                        CSV.File(
                            file["path"],
                            header=header,
                            skipto = header + readrange.start - filerowrange.start + 1,
                            footerskip = filerowrange.stop - readrange.stop,
                        )
                    else
                        error("Expected file with s3_bucket or local path")
                    end,
                ))))
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
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    # TODO: Fix this to read from Arrow format
    # TODO: Fix this to read metadata bout partition sizes
    name = loc_parameters["name"]
    if isdir(name)
        ReadCSV(src, params, batch_idx, batches, comm, "CSV", Dict("files" => [
            Dict("path" => joinpath(name, f))
            for f in sort(readdir(name))
        ]))
    else
        nothing
    end
end

function WriteCSV(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    idx = get_partition_idx(batch_idx, nbatches, comm)
    if haskey(loc_parameters, "s3_bucket")
        # Create bucket if it doesn't exist
        if !loc_parameters["s3_bucket_exists"]
            s3_create_bucket(loc_parameters["s3_bucket"])
        end

        # Write data
        io = IOBuffer()
        CSV.write(io, part)
        s3_put(
            loc_parameters["s3_bucket"],
            joinpath(loc_parameters["s3_key"], "part$idx"),
            take!(io)
        )
    elseif haskey(loc_parameters, "path")
        CSV.write(joinpath(loc_parameters["path", "part$idx"]), part)
    else
        error("Expected location in S3 to write CSV to")
    end
end

function WriteCachedCSV(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    # TODO: Fix this to write metadata for the size of each partition
    WriteCSV(
        src,
        part,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        Dict("path" => loc_parameters["name"])
    )
end

function SplitBlock(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    if isnothing(src)
        src
    else
        split_on_executor(src, params["dim"], batch_idx, nbatches, comm)
    end
end

function merge_on_executor(obj...; dims=1)
    first_obj = first(obj)
    if isa_df(first(src)) && tuple(dims) == (1)
        vcat(src...)
    elseif isa_array(first(src))
        cat(src...; dims=dims)
    else
        error("Expected either AbstractDataFrame or AbstractArray for concatenation")
    end
end

function MergeBlock(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_parameters,
)
    if !isnothing(src)
        src
    else
        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        npartitions = get_npartitions(nbatches, comm)

        # Concatenate across batches
        if batch_idx == 1
            src = []
        end
        push!(src, part)
        if batch_idx == nbatches
            # TODO: Test that this merges correctly
            src = merge_on_executor(src...; dims=params["dim"])

            # Concatenate across workers
            nworkers = get_nworkers(comm)
            if nworkers > 1
                # TODO: Test this case
                io = IOBuffer()
                if isa_df(src)
                    Arrow.write(io, src)
                elseif isa_array(src)
                    serialize(io, src)
                else
                    error("Expected merge of either AbstractDataFrame or AbstractArray")
                end

                # TODO: Maybe avoid some of this in the case of balanced
                src_bytes = view(io.data, 1:position(io))
                sizes = MPI.Allgather(length(src_bytes), comm)
                sum_of_sizes = sum(sizes)
                new_src_chunks_bytes = VBuffer(Array{UInt8}(undef, sum(sizes), sizes))
                MPI.Allgatherv!(src_bytes, new_src_chunks_bytes, comm)
                new_src_chunks = [
                    begin
                        chunk_bytes = view(
                            new_src_chunks_bytes,
                            (new_src_chunks_bytes.displs[i]+1):
                            (new_src_chunks_bytes.displs[i] + new_src_chunks_bytes.counts[i])
                        )
                        if isa_df(src)
                            Arrow.Table(chunk_bytes)
                        else
                            deserialize(chunk_bytes)
                        end
                    end
                    for i in 1:nworkers
                ]
                src = merge_on_executor(new_src_chunks...; dims=params["dim"])
            end
        end

        src
    end
end

Copy(src, args...) = src
