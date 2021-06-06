using Serialization

using MPI

####################
# Helper functions #
####################

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
    split_len(
        src_len,
        get_partition_idx(batch_idx, nbatches, comm),
        get_npartitions(nbatches, comm)
    )

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

# NOTE: This function is copied into locations.jl so any changes here should
# be made there
from_jl_value_contents(jl_value_contents) =
    begin
        io = IOBuffer()
        iob64_decode = Base64DecodePipe(io)
        write(io, jl_value_contents)
        seekstart(io)
        deserialize(iob64_decode)
    end

########################
# Helper MPI functions #
########################

# TODO: Fix below function for reducing values of non-equal sizes
# TODO: Make Allreducev version of below function

# function Reducev(value, op, comm::MPI.Comm)
#     # Reduces values on all processes to a single value on rank 0.
#     # 
#     # This function does the same thing as the function MPI_Reduce using
#     # only MPI_Send and MPI_Recv. As shown, it operates with additions on
#     # integers, so you could trivially use MPI_Reduce, but for operations
#     # on variable size structs for which you cannot define an MPI_Datatype,
#     # you can still use this method, by modifying it to use your op
#     # and your datastructure.

#     # TODO: Actually determine buffer
#     tag = 0
#     size = get_nworkers(comm)
#     rank = get_worker_idx(comm)-1
#     lastpower = 1 << log2(size)

#     # each of the ranks greater than the last power of 2 less than size
#     # need to downshift their data, since the binary tree reduction below
#     # only works when N is a power of two.
#     for i in lastpower:(size-1)
#         if rank == i
#             MPI.send(value, i-lastpower, tag, comm)
#         end
#     for i in 0:(size-lastpower-1)
#         if rank == i
#             MPI.Recv!(recvbuffer, i+lastpower, tag, comm)
#             value = op(value, recvbuffer)
#         end
#     end

#     for d in 0:(fastlog2(lastpower)-1)
#         k = 0
#         while k < lastpower
#             k += 1 << (d + 1)
#         end
#         receiver = k
#         sender = k + (1 << d)
#         if rank == receiver
#             MPI.Recv!(recvbuffer, 1, sender, tag)
#             value = op(value, recvbuffer)
#         elseif rank == sender
#             MPI.Send(value, 1, receiver, tag)
#         end
#     end
#     value
# end

# function fastlog2(v::UInt32)
#     multiply_de_bruijn_bit_position::Vector{Int32} = [
#         0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
#         8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
#     ]

#     v |= v >> 1 # first round down to one less than a power of 2 
#     v |= v >> 2
#     v |= v >> 4
#     v |= v >> 8
#     v |= v >> 16

#     # TODO: Fix this
#     multiply_de_bruijn_bit_position[(UInt32(v * 0x07C4ACDDU) >> 27) + 1]
# end

function tobuf(obj)::Tuple{Symbol, MPI.Buffer}
    # We pass around Julia objects between MPI processes in different ways
    # depending on the data type. For simple isbitstype data we keep it as-is
    # and use the simple C-like data layout for fast transfer. For dataframes,
    # we use Arrow data layout for zero-copy deserialization. For everything
    # else including variably-sized arrays and arbitrary Julia objects, we
    # simply serialize and deserialize using the Serialization module in Julia
    # standard library.

    if isbitstype(obj)
        (:bits, MPI.Buffer(Ref(obj)))
    elseif isa_array(obj) && isbitstype(first(typeof(obj).parameters)) && ndims(obj) == 1
        (:bits, MPI.Buffer(obj))
    elseif isa_df(obj)
        io = IOBuffer()
        Arrow.write(io, obj)
        (:df, Buffer(view(io.data, 1:position(io))))
    else
        io = IOBuffer()
        serialize(io, obj)
        (:unknown, Buffer(view(io.data, 1:position(io))))
    end
end

function buftovbuf(buf::MPI.Buffer, comm::MPI.Comm)::MPI.VBuffer
    # This function expects that the given buf has buf.data being an array.
    # Basically what it does is it takes the result of a call to tobuf above
    # on each process and constructs a VBuffer with the sum of the sizes of the
    # buffers on different processes.
    sizes = MPI.Allgather(length(buf.data), comm)
    VBuffer(similar(buf.data, sum(sizes)), sizes)
end

function bufstosendvbuf(bufs::Vetor{MPI.Buffer}, comm::MPI.Comm)::MPI.VBuffer
    sizes = [length(buf.data) for buf in bufs]
    VBuffer(similar(first(bufs).data, sum(sizes)), sizes)
end

function bufstorecvvbuf(bufs::Vetor{MPI.Buffer}, comm::MPI.Comm)::MPI.VBuffer
    # This function expects that each given buf has buf.data being an array and
    # that the number of bufs in bufs is equal to the size of the communicator.
    sizes = MPI.Allgather(length(buf.data), comm)
    sizes = MPI.Alltoall([length(buf.data) for buf in bufs])
    # NOTE: Ensure that the data fields of the bufs are initialized to have the
    # right data type (e.g., Vector{UInt8} or Vector{Int64})
    # TODO: Don't use similar and instead actually concatentate the data of the
    # bufs
    VBuffer(similar(first(bufs).data, sum(sizes)), sizes)
end

function frombuf(kind, obj)
    if kind == :bits && obj isa Ref
        # TODO: Ensure that the "dereferece" here is necessary
        obj[]
    elseif kind == :bits
        obj
    elseif kind == :df
        DataFrame(Arrow.Table(obj), copycols=false)
    else
        deserialize(obj)
    end
end

function getpath(path)
    if startswith(path, "http://") || startswith(path, "https://")
        # TODO: First check for size of file and only download to
        # disk if it doesn't fit in free memory
        hashed_path = string(hash(path))
        joined_path = joinpath(tempdir(), hashed_path)
        if !isfile(joined_path)
            # NOTE: Even though we are storing in /tmp, this is
            # effectively caching the download. If this is undesirable
            # to a user, a short-term solution is to use a different
            # URL each time (e.g., add a dummy query to the end of the
            # URL)
            download(path, joined_path)
        end
        joined_path
    elseif startswith(path, "s3://")
        replace(path, "s3://", "/mnt/")
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at /mnt/<bucket name>
    else
        path
    end
end

###################################
# Splitting and merging functions #
###################################

# TODO: Implement ReadGroups
# - Computing divisions
# - Distributing divisions among partitions
# - Splitting divisions

function ReadBlock(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # TODO: Avoid redundantly reading in metadata for every single worker-batch
    # TODO: Implement a Read for balanced=false where we can avoid duplicate
    # reading of the same range in different reads

    # Handle single-file nd-arrays
    if haskey(loc_params, "path") && endswith(loc_params["path"], ".h5")
        path = getpath(loc_params["path"])
        f = h5open(path, "r")
        dset = f[loc_params["subpath"]]
        ismapping = false
        # TODO: Implement handling of None
        # TODO: Implement splitting here
        # TODO: Implement writing to location
        if ismmappable(dset)
            ismapping = true
            dset = readmmap(dset)
            close(f)
        else
            dset = split_on_executor(dset, params["dim"], batch_idx, nbatches, comm)
            close(f)
        end
        return 
    end

    # Handle multi-file tabular datasets

    # Handle None location by finding all files in directory used for spilling
    # this value to disk
    if loc_name == "None"
        name = loc_params["name"]
        if isdir(name)
            files = []
            nrows = 0
            for partfilename in readdir(name)
                part_nrows = parse(
                    Int64,
                    replace(split(partfilename, "_nrows=")[end], ".arrow", "")
                )
                push!(
                    files,
                    Dict(
                        "nrows" => part_nrows,
                        "path" => joinpath(name, partfilename)
                    )
                )
                nrows += part_nrows
            end
            loc_params["files"] = files
            loc_params["nrows"] = nrows
        else
            # This is the case where no data has been spilled to disk and this
            # is maybe just an intermediate variable only used for this stage
            return nothing
        end
    end

    # Iterate through files and identify which ones correspond to the range of
    # rows for the batch currently being processed by this worker
    nrows = loc_params["nrows"]
    rowrange = split_len(nrows, batch_idx, nbatches, comm)
    dfs = []
    for file in loc_params["files"]
        newrowsscanned = rowsscanned + file["nrows"]
        filerowrange = (rowsscanned+1):newrowsscanned
        # Check if te file corresponds to the range of rows for the batch
        # currently being processed by this worker
        if isoverlapping(filerowrange, rowrange)
            # Deterine path to read from
            path = getpath(file["path"])

            # Read from location depending on data format
            readrange = max(rowrange.start, filerowrange.start):min(rowrange.stop, filerowrange.stop)
            header = 1
            if endswith(path, ".csv")
                f = CSV.File(
                    path,
                    header=header,
                    skipto = header + readrange.start - filerowrange.start + 1,
                    footerskip = filerowrange.stop - readrange.stop,
                )
                push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
            elseif endswith(path, ".parquet")
                f = Parquet.File(
                    path,
                    rows=(readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1)
                )
                push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
            elseif endswith(path, ".arrow")
                rbrowrange = filerowrange.start:(filerowrange.start-1)
                for tbl in Arrow.Stream(path)
                    rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+length(tbl))
                    if isoverlapping(rbrowrange, rowrange)
                        readrange = max(rowrange.start, rbrowrange.start):min(rowrange.stop, rbrowrange.stop)
                        df = DataFrame(tbl)
                        df = df[(readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1), :]
                        push!(dfs, df)
                    end
                end
            else
                error("Expected CSV or Parquet or Arrow format")
            end
        end
        rowsscanned = newrowsscanned
    end

    # Concatenate and return
    vcat(dfs...)
end

function Write(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # Get path of directory to write to
    path = if loc_name == "None" loc_params["name"] else loc_params["path"] end
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = replace(path, "s3://", "/mnt/")
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at /mnt/<bucket name>
    end

    # Write file for this partition
    idx = get_partition_idx(batch_idx, nbatches, comm)
    if isa_df(part)
        # Create directory if it doesn't exist
        # TODO: Avoid this and other filesystem operations that would be costly
        # since S3FS is being used
        if !isdir(path)
            mkpath(path)
        end

        nrows = size(part, 1)
        partfilepath = joinpath(path, "part$idx" * "_nrows=$nrows.arrow"),
        Arrow.write(partfilepath, part)
        src
        # TODO: Delete all other part* files for this value if others exist
    elseif isa_array(part)
        # TODO: Check if HDF5 dataset is created. If not, wait for the master
        # node to create it.
    else
        error("Only Array or DataFrame is currently supported for writing as Block")
    end
end

function SplitBlock(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if isnothing(src)
        src
    else
        split_on_executor(src, params["dim"], batch_idx, nbatches, comm)
    end
end

function merge_on_executor(obj...; dims=1)
    first_obj = first(obj)
    if isa_df(first_obj) && tuple(dims) == (1)
        vcat(obj...)
    elseif isa_array(first_obj)
        cat(obj...; dims=dims)
    else
        error("Expected either AbstractDataFrame or AbstractArray for concatenation")
    end
end

function merge_on_executor(kind::Symbol, vbuf::MPI.VBuffer, nchunks::Integer; dims=1)
    chunk = [
        begin
            chunk = view(
                vbuf.data,
                (vbuf.displs[i]+1):
                (vbuf.displs[i] + vbuf.counts[i])
            )
            if kind == :df
                DataFrame(Arrow.Table(chunk))
            elseif kind == :bits
                chunk
            else
                deserialize(IOBuffer(chunk))
            end
        end
        for i in 1:nchunks
    ]
    src = merge_on_executor(new_src_chunks...; dims)
end

function Merge(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if isnothing(src)
        # We only need to concatenate partitions if the source is nothing.
        # Because if the source is something, then part must be a view into it
        # and no data movement is needed.

        dim = isa_array(part) ? params["key"] : 1

        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        npartitions = get_npartitions(nbatches, comm)

        # Concatenate across batches
        if batch_idx == 1
            src = []
        end
        push!(src, part)
        if batch_idx == nbatches
            # TODO: Test that this merges correctly
            src = merge_on_executor(src...; dims=dim)

            # Concatenate across workers
            nworkers = get_nworkers(comm)
            if nworkers > 1
                src = Consolidate(src, Dict("key" => params["key"]), Dict(), comm)
            end
        end
    end

    src
end

function CopyFrom(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if loc_name == "Value"
        loc_params["value"]
    elseif loc_name == "None"
        p = joinpath(loc_params["name"], "_part")
        if isfile(p)
            # Check if there is a single partition spilled to disk,
            # indicating that we should then simply deserialize and return
            open(p) do f
                deserialize(f)
            end
        elseif isdir(loc_params["name"])
            ReadAsBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        else
            nothing
        end
    elseif loc_name == "Remote"
        ReadAsBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    elseif loc_name == "Executor"
        src
    end
end

function CopyTo(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if loc_name == "Memory"
        src = part
    elseif get_partition_idx(batch_idx, nbatches, comm) == 1
        # If we are copying to an external location we only want to do it on
        # the first worker since assuming that `on` is either `everywhere` or
        # `head`, so any batch on the first worker is guaranteed to have the
        # value that needs to be copied (either spilled to disk if None or
        # sent to remote storage).
        if loc_name == "Disk"
            # TODO: Add case for an array by writing to HDF5 dataset
            if isa_df(part) || isa_array(part)
                WriteAsBlock(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
            else
                p = joinpath(loc_params["path"], "_part")
                open(p) do f
                    serialize(f, part)
                end
            end
        elseif loc_name == "Remote"
            WriteAsBlock(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        else
            error("Unexpected location")
        end
    else
    src
end

function ReduceAndCopyTo(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # Merge reductions from batches
    op = src_params["reducer"]
    op = src_params["with_key"] ? op(src_params["key"]) : op
    src = op(src, part)

    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, Dict("reducer" => op), Dict(), comm)
    end

    if loc_name != "Memory"
        CopyTo(src, part, params, batch_idx, nbatches, comm, loc_name, loc_params)
    end

    src
end

ReduceWithKeyAndCopyTo = ReduceAndCopyTo

function Divide(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    length(split_len(part, batch_idx, nbatches, comm))
end

#####################
# Casting functions #
#####################

function Reduce(
    part,
    src_params,
    dst_params,
    comm
)
    # Get operator for reduction
    op = src_params["reducer"]
    op = src_params["with_key"] ? op(src_params["key"]) : op
    src = op(src, part)

    # Get buffer to reduce
    kind, sendbuf = tobuf(part)
    # TODO: Handle case where different processes have differently sized
    # sendbuf
    
    # Perform reduction
    part = MPI.Allreduce(
        sendbuf.data,
        (a, b) -> begin
            tobuf(op(frombuf(kind, a), frombuf(kind, b)))[2]
        end,
        comm,
    )
    part
end

ReduceWithKey = Reduce

function Rebalance(
    part,
    src_params,
    dst_params,
    comm
)
    # TODO: Implement
end

function Distribute(
    part,
    src_params,
    dst_params,
    comm
)
    dim = isa_array(part) ? dst_params["key"] : 1
    part = copy(split_on_executor(part, dim, 1, 1, comm))
    part
end

function Consolidate(
    part,
    src_params,
    dst_params,
    comm
)
    dim = isa_array(part) ? src_params["key"] : 1
    kind, sendbuf = tobuf(part)
    recvvbuf = buftovbuf(sendbuf)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized
    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    part = merge_on_executor(kind, recvvbuf, get_nworkers(comm), comm; dims=dim)
    part
end

function rebalance_on_executor(
    part,
    src_params,
    dst_params,
    nbatches::Integer,
    comm
)
    # Get the range owned by this worker
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    len = size(part, src_params["dim"])
    startidx = MPI.Exscan(len, +, comm)
    if worker_idx == 1
        startidx = 1
    else
        startidx += 1
    end
    endidx = startidx + len - 1
    
    # Send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    npartitions = nbatches * nworkers
    whole_len = MPI.bcast(endidx, nworkers-1, comm)
    for partition_idx in npartitions
        split_len(whole_len, partition_idx, npartitions)
    end

    # TODO: Finish
end

function Shuffle(
    part,
    src_params,
    dst_params,
    comm
)

end

# NOTE: This is duplicated between pt_lib.jl and the client library
orderinghash(x::Any) = x # This lets us handle numbers and dates
orderinghash(s::String) = Integer.(codepoint.(first(s, 32) * repeat(" ", 32-length(s))))
orderinghash(A::Array) = orderinghash(first(A))

function get_divisions(divisions, npartitions)
    # This function accepts a list of divisions where each division is a tuple
    # of ordering hashes (values returned by `orderinghash` which are either
    # numbers or vectors of numbers). It also accepts a number of partitions to
    # produce divisions for. The result is a list of length `npartitions`
    # containing lists of divisions for each partition. A partition may contain
    # multiple divisions.

    ndivisions = length(divisions)
    if ndivisions >= npartitions
        # If there are more divisions than partitions, we can distribute them
        # easily. Each partition gets 0 or more divisions.
        ndivisions_per_partition = div(ndivisions, npartitions)
        [
            begin
                islastpartition = partition_idx == npartitions
                firstdivisioni = ((partition_idx-1) * ndivisions_per_partition) + 1
                lastdivisioni = islastpartition ? ndivisions : partition_idx * ndivisions_per_partition
                divisions[firstdivisioni:lastdivisioni]
            end
            for partition_idx in 1:npartitions
        ]
    else
        # Otherwise, each division must be shared among 1 or more partitions
        allsplitdivisions = []
        npartitions_per_division = div(npartitions, ndivision)

        # Iterate through the divisions and split each of them and find the
        # one that contains a split that this partition must own and use as
        # its `partition_divisions`
        for (division_idx, division) in enumerate(divisions)
            # Determine the range (from `firstpartitioni` to `lastpartitioni`) of
            # partitions that own this division
            islastdivision = division_idx == ndivisions
            firstpartitioni = ((division_idx-1) * npartitions_per_division) + 1
            lastpartitioni = islastdivision ? npartitions : division_idx * npartitions_per_division
            partitionsrange = firstpartitioni:lastpartitioni

            # # If the current partition is in that division, compute the
            # # subdivision it should use for its partition
            # if partition_idx in partitionsrange

            # We need to split the division among all the partitions in
            # its range
            ndivisionsplits = length(partitionsrange)

            # Get the `Vector{Number}`s to interpolate between
            divisionbegin = to_vector(first(division))
            divisionend = to_vector(last(division))

            # Initialize divisions for each split
            splitdivisions = repeat([[copy(divisionbegin), copy(divisionbegin)]], ndivisionsplits)

            # Adjust the divisions for each split to interpolate
            for (i, (dbegin, dend)) in enumerate(zip(divisionbegin, divisionend))
                # Find the first index in the `Vector{Number}` where
                # there is a difference that we can interpolate between
                if dbegin != dend
                    dpersplit = div(dend-dbegin, ndivisionsplits)
                    for j in 1:ndivisionsplits
                        # Update the start and end of the division
                        islastsplit = j == ndivisionsplits
                        splitdivisions[j][1] = dbegin + dpersplit * (j-1)
                        splitdivisions[j][2] = islastsplit ? dend : dbegin + dpersplit * j
                    end

                    # Stop if we have found a difference we can
                    # interpolate between
                    # TODO: If the difference is not that much,
                    # interpolate between multiple consecutive
                    # differeing characters together
                    break
                end
            end

            # Convert back to `Number` if the divisions were originally
            # `Number`s
            if !(first(division) isa Vector)
                splitdivisions = [
                    # NOTE: When porting this stuff to Python, be sure
                    # to take into account the fact that Julia treats
                    # many values as arrays
                    (first(splitdivisionbegin), first(splitdivisionend))
                    for (splitdivisionbegin, splitdivisionend) in splitdivisions
                ]
            end

            # # Get the split of the division that this partition should own
            # splitdivision = splitdivisions[1+partition_idx-first(partitionsrange)]

            # # Stop because we have found a division that this partition
            # # is supposed to own a split from
            # break

            # Each partition must have a _list_ of divisions so we must have a list
            # for each partition
            for splitdivision in splitdivisions
                push!(allsplitdivisions, [splitdivision])
            end

            # end
        end
        allsplitdivisions
    end
end

function get_partition_idx_from_divisions(val, divisions)
    # The given divisions may be returned from `get_divisions`
    for (i, div) in enumerate(divisions)
        if val >= first(div)[1] && val < last(div)[2]
            return i
        end
    end
    throw(Error("Given value does not belong to any of the given divisions"))
end

function DistributeAndShuffle(
    part,
    src_params,
    dst_params,
    comm
)
    # Distribute the divisions among the partitions
    divisions = dst_params["divisions"]
    ndivisions = length(divisions)
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    islastworker = worker_idx == nworkers
    worker_divisions =
        if ndivisions >= nworkers
            # If there are more divisions than workers, we can distribute them
            # easily
            ndivisions_per_worker = div(ndivisions, nworkers)
            firstdivisioni = ((worker_idx-1) * ndivisions_per_worker) + 1
            lastdivisioni = islastworker ? ndivisions : worker_idx * ndivisions_per_worker
            divisions[firstdivisioni:lastdivisioni]
        else
            # Otherwise, each division must be shared among 1 or more workers
            splitdivision = nothing
            nworkers_per_division = div(nworkers, ndivision)

            # Iterate through the divisions and split each of them and find the
            # one that contains a split that this worker must own and use as
            # its `worker_divisions`
            for (division_idx, division) in enumerate(divisions)
                # Determine the range (from `firstworkeri` to `lastworkeri`) of
                # workers that own this division
                islastdivision = division_idx == ndivisions
                firstworkeri = ((division_idx-1) * nworkers_per_division) + 1
                lastworkeri = islastdivision ? nworkers : division_idx * nworkers_per_division
                workersrange = firstworkeri:lastworkeri

                # If the current worker is in that division, compute the
                # subdivision it should use for its partition
                if worker_idx in workersrange
                    # We need to split the division among all the workers in
                    # its range
                    ndivisionsplits = length(workersrange)

                    # Get the `Vector{Number}`s to interpolate between
                    divisionbegin = to_vector(first(division))
                    divisionend = to_vector(last(division))

                    # Initialize divisions for each split
                    splitdivisions = repeat([[copy(divisionbegin), copy(divisionbegin)]], ndivisionsplits)

                    # Adjust the divisions for each split to interpolate
                    for (i, (dbegin, dend)) in enumerate(zip(divisionbegin, divisionend))
                        # Find the first index in the `Vector{Number}` where
                        # there is a difference that we can interpolate between
                        if dbegin != dend
                            dpersplit = div(dend-dbegin, ndivisionsplits)
                            for j in 1:ndivisionsplits
                                # Update the start and end of the division
                                islastsplit = j == ndivisionsplits
                                splitdivisions[j][1] = dbegin + dpersplit * (j-1)
                                splitdivisions[j][2] = islastsplit ? dend : dbegin + dpersplit * j
                            end

                            # Stop if we have found a difference we can
                            # interpolate between
                            # TODO: If the difference is not that much,
                            # interpolate between multiple consecutive
                            # differeing characters together
                            break
                        end
                    end

                    # Convert back to `Number` if the divisions were originally
                    # `Number`s
                    if !(first(division) isa Vector)
                        splitdivisions = [
                            # NOTE: When porting this stuff to Python, be sure
                            # to take into account the fact that Julia treats
                            # many values as arrays
                            (first(splitdivisionbegin), first(splitdivisionend))
                            for (splitdivisionbegin, splitdivisionend) in splitdivisions
                        ]
                    end

                    # Get the split of the division that this worker should own
                    splitdivision = splitdivisions[1+worker_idx-first(workersrange)]


                    # Stop because we have found a division that this worker
                    # is supposed to own a split from
                    break
                end
            end
            [splitdivision]
        end
    in_worker_divisions = val -> begin
        oh = orderinghash(val)
        oh >= first(worker_divisions) && oh < last(worker_divisions)
    end

    # TODO: Use in_worker_divisions to filter for the data to return or to
    # group-by and split and distribute

    key = dst_params["key"]
    transform!(divisions, Symbol(key) =>  => :banyan_divisions)

    part = copy(split_on_executor(part, dim, 1, 1, comm))
    part
end