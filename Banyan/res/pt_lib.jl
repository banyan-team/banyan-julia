# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pt_lib.jl` should have a corresponding
# `pt_lib_info.json` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

using Serialization
using Base64

using MPI

include("utils.jl")

###################################
# Splitting and merging functions #
###################################

# TODO: Implement ReadGroups
# - Computing divisions
# - Distributing divisions among partitions
# - Splitting divisions

ReturnNull(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = nothing

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
    if (loc_name == "Disk" && isfile(loc_params["path"])) ||
        (loc_name == "Remote" && endswith(loc_params["path"], ".h5"))

        path = getpath(loc_params["path"])
        f = h5open(path, "w")
        dset = loc_name == "Disk" ? f["part"] : f[loc_params["subpath"]]

        ismapping = false
        # TODO: Use `view` instead of `getindex` in the call to
        # `split_on_executor` here if HDF5 doesn't support this kind of usage
        if ismmappable(dset)
            ismapping = true
            dset = readmmap(dset)
            close(f)
            dset = split_on_executor(dset, params["key"], batch_idx, nbatches, comm)
        else
            dset = split_on_executor(dset, params["key"], batch_idx, nbatches, comm)
            close(f)
        end
        return dset
    end

    # Handle multi-file tabular datasets

    # Handle None location by finding all files in directory used for spilling
    # this value to disk
    if loc_name == "Disk"
        name = loc_params["path"]
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

splitting_divisions = IdDict()

function ReadGroup(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # TODO: Store filters in parameters of the PT and use them to do
    # partition pruning, avoiding reads that are unnecessary

    # Get information needed to read in the appropriate group
    divisions = params["divisions"]
    key = params["key"]
    nworkers = get_nworkers(comm)
    npartitions = nworkers * nbatches
    partition_divisions = get_divisions(divisions, npartitions)
    
    # Get the divisions that are relevant to this batch by iterating
    # through the divisions in a stride and consolidating the list of divisions
    # for each partition. Then, ensure we use boundedlower=true only for the
    # first batch and boundedupper=true for the last batch.
    curr_partition_divisions = []
    for worker_division_idx in 1:nworkers
        for batch_division_idx in 1:nbatches
            partition_division_idx = (worker_division_idx-1)*nbatches + batch_division_idx
            if j == batch_idx
                p_divisions = partition_divisions[partition_division_idx]
                push!(curr_partition_divisions, (first(p_divisions)[1], last(p_divisions)[2]))
            end
        end
    end

    # Read in each batch and shuffle it to get the data for this partition
    parts = []
    for i in 1:nbatches
        # Read in data for this batch
        part = ReadBlock(src, params, i, nbatches, comm, loc_name, loc_params)

        # Shuffle the batch and add it to the set of data for this partition
        push!(parts, Shuffle(
            part,
            Dict(),
            Dict("key" => key, "divisions" => curr_partition_divisions),
            comm,
            boundedlower = i == 1,
            boundedupper = i == nbatches,
        ))
    end

    # Concatenate together the data for this partition
    res = merge_on_executor(parts, dims=isa_array(first(parts)) ? key : 1)

    # Store divisions
    global splitting_divisions
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    splitting_divisions[res] =
        (partition_divisions[partition_idx], partition_idx == 1, partition_idx == npartitions)

    res
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
    path = loc_params["path"]
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = replace(path, "s3://", "/home/ec2-user/mnt/s3/")
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
        partfilepath = joinpath(path, "_part$idx" * "_nrows=$nrows.arrow"),
        Arrow.write(partfilepath, part)
        src
        # TODO: Delete all other part* files for this value if others exist
    elseif isa_array(part)
        dim = params["key"]
        whole_size = MPI.Reduce(
            size(part),
            indexapply(+, a, b, index=dim),
            0,
            comm,
        )

        # TODO: Check if HDF5 dataset is created. If not, wait for the master
        # node to create it.
        if get_partition_idx(batch_idx, nbatches, comm) == 1
            hfopen(joinpath(path * "_part"), "w") do fid
                create_dataset(fid, "part", similar(part, whole_size))
            end
            touch(joinpath(path * "_is_ready"))
        end

        # Wait till the dataset is created
        while !isfile(joinpath(path * "_is_ready")) end

        # Write part to the dataset
        f = h5open(joinpath(path * "_part"), "w")
        dset = loc_name == "Disk" ? f["part"] : f[loc_params["subpath"]]
        # TODO: Use `view` instead of `getindex` in the call to
        # `split_on_executor` here if HDF5 doesn't support this kind of usage
        if ismmappable(dset)
            dset = readmmap(dset)
            close(f)
            dsubset = split_on_executor(dset, dim, batch_idx, nbatches, comm)
            dsubset .= part
        else
            dsubset = split_on_executor(dset, dim, batch_idx, nbatches, comm)
            dsubset .= part
            close(f)
        end

        # TODO: Make this work for variable-sized element type
        # TODO: Support arrays and reductions with variable-sized elements
        # TODO: Maybe use Arrow
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
        split_on_executor(src, params["key"], batch_idx, nbatches, comm)
    end
end

function SplitGroup(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)

    # Get divisions stored with src
    global splitting_divisions
    divisions, boundedlower, boundedupper = splitting_divisions[src]

    # Call DistributeAndShuffle on src
    key = params["key"]
    rev = params["rev"]
    # TODO: Do a single groupby here instead of a filter for each partition
    res = DistributeAndShuffle(
        part,
        Dict(),
        Dict("key" => key, "divisions" => divisions, "rev" => rev),
        comm,
        boundedlower,
        boundedupper
    )

    # Store divisions
    global splitting_divisions
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    splitting_divisions[res] = (
        partition_divisions[partition_idx],
        boundedlower && partition_idx == 1,
        boundedupper && partition_idx == npartitions,
    )

    res
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
    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
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
            # src = merge_on_executor(src...; dims=dim)
            cat(src...; dims=dim)

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
    elseif loc_name == "Disk"
        p = joinpath(loc_params["path"] * "_part")
        if isfile(p)
            # Check if there is a single partition spilled to disk,
            # indicating that we should then simply deserialize and return
            open(p) do f
                deserialize(f)
            end
        elseif isdir(loc_params["path"])
            Read(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        else
            nothing
        end
    elseif loc_name == "Remote"
        Read(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    elseif loc_name == "Client" && get_partition_idx(batch_idx, nbatches, comm) == 1
        receive_from_client(loc_params["value_id"])
    elseif loc_name == "Memory"
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
                Write(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
            else
                p = joinpath(loc_params["path"] * "_part")
                open(p, "w") do f
                    serialize(f, part)
                end
            end
        elseif loc_name == "Remote"
            Write(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        elseif loc_name == "Client"
            # TODO: Ensure this only sends once
            send_to_client(loc_params["value_id"], part)
        else
            error("Unexpected location")
        end
    end
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
    op = params["reducer"]
    op = params["with_key"] ? op(params["key"]) : op
    src = isnothing(src) ? part : op(src, part)

    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, params, Dict(), comm)
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
    dim = dst_params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    if part isa Tuple
        newpartdim = length(split_len(part[dim], batch_idx, nbatches, comm))
        indexapply(_->newpartdim, part, index=dim)
    else
        length(split_len(part[dim], batch_idx, nbatches, comm))
    end
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

    # Get buffer to reduce
    # kind, sendbuf = tobuf(part)
    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

    # @show kind
    
    # Perform reduction
    part = MPI.Allreduce(
        part,
        # sendbuf,
        # (a, b) -> begin
        #     # tobuf(op(frombuf(kind, a), frombuf(kind, b)))[2]
        #     op(a, b)
        # end,
        op,
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
    # Get the range owned by this worker
    dim = isa_array(part) ? dst_params["key"] : 1
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    len = size(part, dim)
    scannedstartidx = MPI.Exscan(len, +, comm)
    startidx = worker_idx == 1 ? 1 : scannedstartidx + 1
    endidx = startidx + len - 1

    # Get functions for serializing/deserializing
    ser = isa_array(part) ? serialize : Arrow.write
    de = isa_array(part) ? (IOBuffer |> deserialize) : (Arrow.Table |> DataFrame)
    
    # Construct buffer to send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    npartitions = nbatches * nworkers
    whole_len = MPI.bcast(endidx, nworkers-1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts = []
    for partition_idx in npartitions
        # `split_len` gives us the range that this partition needs
        partitionrange = split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap = max(startidx, partitionrange.start) <= min(endidx, partitionrange.start.stop)

        # If they do overlap, then serialize the overlapping slice
        if rangesoverlap
            ser(
                io,
                view(
                    part,
                    fill(:, dim - 1)...,
                    max(1, partitionrange.start - startidx + 1):min(
                        size(part, dim),
                        partitionrange.end - startidx + 1,
                    ),
                    fill(:, ndims(part) - dim)...,
                ),
            )
        end

        # Add the count of the size of this chunk in bytes
        push!(counts, io.position - nbyteswritten)
        nbyteswritten = io.position
    end
    sendbuf = VBuffer(MPI.Buffer(view(io.data, 1:nbyteswritten)), counts)

    # Create buffer for receiving pieces
    # TODO: Refactor the intermediate part starting from there if we add
    # more cases for this function
    sizes = MPI.Alltoall(counts)
    recvbuf = VBuffer(similar(io.data, sum(sizes)), sizes)

    # Return the concatenated array
    cat([
        de(view(recvbuf.data, displ+1:displ+count))
        for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
    ]...; dims=dim)
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
    recvvbuf = buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized
    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    part = merge_on_executor(kind, recvvbuf, get_nworkers(comm), comm; dims=dim)
    part
end
function DistributeAndShuffle(
    part,
    src_params,
    dst_params,
    comm;
    boundedlower=false,
    boundedupper=false
)
    # Get the divisions to apply
    divisions = dst_params["divisions"]
    key = dst_params["key"]
    rev = dst_params["rev"]
    ndivisions = length(divisions)
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    worker_divisions = get_divisions(divisions, nworkers)
    if rev
        reverse!(worker_divisions)
    end

    # Create helper function for getting index of partition that owns a given
    # value
    partition_idx_getter(val) = get_partition_idx_from_divisions(
        val,
        worker_divisions,
        boundedlower,
        boundedupper
    )

    # Apply divisions to get only the elements relevant to this worker
    if isa_df(part)
        filter(row -> partition_idx_getter(row[key]) == worker_idx, part)
    elseif isa_array(part)
        cat(
            filter(
                e -> partition_idx_getter(e) == worker_idx,
                eachslice(part, dims = key),
            )...;
            dims = key,
        )
    else
        throw(ArgumentError("Expected array or dataframe to distribute and shuffle"))
    end
end

function Shuffle(
    part,
    src_params,
    dst_params,
    comm;
    boundedlower=false,
    boundedupper=false
)
    # Get the divisions to apply
    divisions = dst_params["divisions"] # list of min-max tuples
    key = dst_params["key"]
    ndivisions = length(divisions)
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    worker_divisions = get_divisions(divisions, nworkers) # list of min-max tuple lists
    if rev
        reverse!(worker_divisions)
    end

    # Perform shuffle
    partition_idx_getter(val) = get_partition_idx_from_divisions(
        val,
        worker_divisions,
        boundedlower,
        boundedupper
    )
    if isa_df(part)
        # Compute the partition to send each row of the dataframe to
        transform!(part, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)

        # Group the dataframe's rows by what partition to send to
        gdf = groupby(part, :banyan_shuffling_key, sort=true)

        # Create buffer for sending dataframe's rows to all the partitions
        io = IOBuffer()
        nbyteswritten = 0
        counts = []
        for partition_idx in 1:nworkers
            Arrow.write(io, partition_idx in keys(gdf) ? gdf[partition_idx] : DataFrame())
            push!(counts, io.position - nbyteswritten)
            nbyteswritten = io.position
        end
        sendbuf = VBuffer(MPI.Buffer(view(io.data, 1:nbyteswritten)), counts)

        # Create buffer for receiving pieces
        sizes = MPI.Alltoall(counts)
        recvbuf = VBuffer(similar(io.data, sum(sizes)), sizes)

        # Return the concatenated dataframe
        res = vcat([
            DataFrame(Arrow.Table(view(recvbuf.data, displ+1:displ+count)), copycols=false)
            for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
        ])
        select!(res, Not(:banyan_shuffling_key))
        res
    elseif isa_array(part)
        # Group the data along the splitting axis (specified by the "key"
        # parameter)
        partition_idx_to_e = [[] for partition_idx in 1:nworkers]
        for e in eachslice(part, dims=key)
            partition_idx = get_partition_idx_from_divisions(e, worker_divisions)
            push!(partition_idx_to_e[partition_idx], e)
        end

        # Construct buffer for sending data
        io = IOBuffer()
        nbyteswritten = 0
        counts = []
        for partition_idx in 1:nworkers
            # TODO: If `isbitstype(eltype(e))`, we may want to pass it in
            # directly as an MPI buffer (if there is such a thing) instead of
            # serializing
            # Append to serialized buffer
            e = partition_idx_to_e[partition_idx]
            if !isempty(e)
                serialize(io, cat(e...; dims=key))
            end

            # Add the count of the size of this chunk in bytes
            push!(counts, io.position - nbyteswritten)
            nbyteswritten = io.position
        end
        sendbuf = VBuffer(MPI.Buffer(view(io.data, 1:nbyteswritten)), counts)

        # Create buffer for receiving pieces
        # TODO: Refactor the intermediate part starting from there if we add
        # more cases for this function
        sizes = MPI.Alltoall(counts)
        recvbuf = VBuffer(similar(io.data, sum(sizes)), sizes)

        # Return the concatenated array
        cat([
            deserialize(IOBuffer(view(recvbuf.data, displ+1:displ+count)))
            for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
        ]...; dims=key)
    else
        throw(ArgumentError("Expected array or dataframe to distribute and shuffle"))
    end
end