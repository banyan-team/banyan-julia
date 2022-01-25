function read_julia_array_file(path, header, rowrange, readrange, filerowrange, dfs, dim)
    push!(
        dfs,
        let arr = deserialize(path)
            arr[
                [
                    if i == dim
                        Colon()
                    else
                        (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1)
                    end
                    for i in 1:ndims(arr)
                ]...
            ]
        end
    )
end

function ReadBlockJuliaArray(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # TODO: Implement a Read for balanced=false where we can avoid duplicate
    # reading of the same range in different reads

    path = Banyan.getpath(loc_params["path"])

    # Handle multi-file tabular datasets

    # Handle None location by finding all files in directory used for spilling
    # this value to disk
    loc_name == "Disk" || error("Reading from Julia-serialized arrays is only supported for local disk storage")
        
    # TODO: Only collect files and nrows info for this location associated
    # with a unique name corresponding to the value ID - only if this is
    # the first batch or loop iteration.
    name = loc_params["path"]
    name_path = path
    # TODO: isdir might not work for S3FS
    isdir(name_path) || error("Expected $path to be a directory containing files of Julia-serialized arrays")

    files = []
    nrows = 0
    dim_partitioning = params["key"]
    dim = -1
    for partfilename in readdir(name_path)
        if partfilename != "_metadata"
            if dim == -1
                dim = parse(Int64, partfilename[5:findfirst("_", partfilename).start-1])
            end
            part_nrows = parse(
                Int64,
                split(partfilename, "_nslices=")[end],
            )
            push!(
                files,
                Dict("nrows" => part_nrows, "path" => joinpath(name, partfilename)),
            )
            nrows += part_nrows
        end
    end
    dim > 0 || error("Unable to find dimension of Julia-serialized array stored in directory $name_path")
    partitioned_on_dim = dim == dim_partitioning
    loc_params["files"] = files
    loc_params["nrows"] = nrows

    # Iterate through files and identify which ones correspond to the range of
    # rows for the batch currently being processed by this worker
    metadata = nothing
    nrows = if partitioned_on_dim
        loc_params["nrows"]
    else
        metadata = deserialize(
            joinpath(name_path, "_metadata")
        )
        metadata["sample_size"][dim_partitioning]
    end
    rowrange = Banyan.split_len(nrows, batch_idx, nbatches, comm)
    dfs = AbstractArray[]
    rowsscanned = 0
    for file in sort(loc_params["files"], by = filedict -> filedict["path"])
        newrowsscanned = rowsscanned + file["nrows"]
        filerowrange = (rowsscanned+1):newrowsscanned
        # Check if the file corresponds to the range of rows for the batch
        # currently being processed by this worker
        if !partitioned_on_dim || Banyan.isoverlapping(filerowrange, rowrange)
            # Deterine path to read from
            file_path = file["path"]
            path = Banyan.getpath(file_path)

            # Read from location depending on data format
            readrange = if partitioned_on_dim
                max(rowrange.start, filerowrange.start):min(
                    rowrange.stop,
                    filerowrange.stop,
                )
            else
                rowrange
            end
            header = 1
            # TODO: Scale the memory usage appropriately when splitting with
            # this and garbage collect if too much memory is used.
            read_julia_array_file(path, header, rowrange, readrange, filerowrange, dfs, dim)
        end
        rowsscanned = newrowsscanned
    end

    # Concatenate and return
    # NOTE: If this partition is empty, it is possible that the result is
    # schemaless (unlike the case with HDF5 where the resulting array is
    # guaranteed to have its ndims correct) and so if a split/merge/cast
    # function requires the schema (for example for grouping) then it must be
    # sure to take that account
    res = if isempty(dfs)
        if isnothing(metadata)
            metadata = deserialize(
                joinpath(name_path, "_metadata")
            )
        end
        sample_size = metadata["sample_size"]
        actual_size = indexapply(_ -> nrows, sample_size; index=dim)
        actual_part_size = indexapply(_ -> 0, actual_size; index=dim_partitioning)
        eltype = metadata["eltype"]
        Base.Array{eltype}(undef, actual_part_size)
    elseif length(dfs) == 1
        dfs[1]
    else
        cat(dfs...; dims=dim_partitioning)
    end
    res
end

ReadGroupJuliaArray = Banyan.ReadGroup(ReadBlockJuliaArray)

function write_file_julia_array(part, path, dim, sortableidx, nrows)
    serialize(
        joinpath(path, "dim=$dim" * "_part$sortableidx" * "_nslices=$nrows"),
        part
    )
end

function write_metadata_for_julia_array(actualpath, part)
    serialize(
        joinpath(actualpath, "_metadata"),
        Dict(
            "sample_size" => size(part),
            "eltype" => eltype(part)
        )
    )
end

function WriteJuliaArray(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # Get rid of splitting divisions if they were used to split this data into
    # groups
    splitting_divisions = Banyan.get_splitting_divisions()
    delete!(splitting_divisions, part)

    # Get path of directory to write to
    path = loc_params["path"]
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = Banyan.getpath(path)
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else
        # Prepend "efs/" for local paths
        path = Banyan.getpath(path)
    end

    # Write file for this partition
    worker_idx = Banyan.get_worker_idx(comm)
    idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
    actualpath = deepcopy(path)
    if nbatches > 1
        # Add _tmp to the end of the path
        path = path * "_tmp"
    end

    # TODO: Delete existing files that might be in the directory but first
    # finish writing to a path*"_new" directory and then linking ... or 
    # something like that. Basically we need to handle the case where we
    # have batching in the first PT and we are reading from and writing to
    # the same directory.
    # 1. Write all output to a new directory
    # 2. On the last partition, do a barrier (or a gather) and then delete
    # the old directory
    # 3. Do another barrier on the last batch and delete the old directory
    # and link to the new one

    # NOTE: This is only needed because we might be writing to the same
    # place we are reading from. And so we want to make sure we finish
    # reading before we write the last batch
    if batch_idx == nbatches
        MPI.Barrier(comm)
    end

    if worker_idx == 1
        if nbatches == 1
            # If there is no batching we can delete the original directory
            # right away. Otherwise, we must delete the original directory
            # only at the end.
            # TODO: When refactoring the locations, think about how to allow
            # stuff in the directory
            Banyan.rmdir_on_nfs(actualpath)
        end

        # Create directory if it doesn't exist
        # TODO: Avoid this and other filesystem operations that would be costly
        # since S3FS is being used
        if batch_idx == 1
            Banyan.rmdir_on_nfs(path)
            mkpath(path)
        end

        if nbatches == 1
            write_metadata_for_julia_array(actualpath, part)
        end
    end
    MPI.Barrier(comm)

    dim = params["key"]
    nrows = size(part, dim)
    sortableidx = Banyan.sortablestring(idx, get_npartitions(nbatches, comm))
    write_file_julia_array(part, path, dim, sortableidx, nrows)
    MPI.Barrier(comm)
    if nbatches > 1 && batch_idx == nbatches
        tmpdir = readdir(path)
        if worker_idx == 1
            Banyan.rmdir_on_nfs(actualpath)
            mkpath(actualpath)
        end
        MPI.Barrier(comm)
        for batch_i = 1:nbatches
            idx = Banyan.get_partition_idx(batch_i, nbatches, worker_idx)
            tmpdir_idx = findfirst(fn -> contains(fn, "part$idx"), tmpdir)
            if !isnothing(tmpdir_idx)
                tmpsrc = joinpath(path, tmpdir[tmpdir_idx])
                actualdst = joinpath(actualpath, tmpdir[tmpdir_idx])
                cp(tmpsrc, actualdst, force=true)
            end
        end
        MPI.Barrier(comm)
        if worker_idx == 1
            Banyan.rmdir_on_nfs(path)
            write_metadata_for_julia_array(actualpath, part)
        end
        MPI.Barrier(comm)
    end
    # TODO: Store the number of rows per file here with some MPI gathering
    src
    # TODO: Delete all other part* files for this value if others exist
end

CopyFromJuliaArray(src, params, batch_idx, nbatches, comm, loc_name, loc_params) = begin
    params["key"] = 1
    ReadBlockJuliaArray(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyToJuliaArray(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteJuliaArray(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

function ReadBlockHDF5(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # Handle single-file nd-arrays
    # We check if it's a file because for items on disk, files are HDF5
    # datasets while directories contain Parquet, CSV, or Arrow datasets
    path = Banyan.getpath(loc_params["path"])
    println("In ReadBlockHDF5 with path=$path, loc_name=$loc_name, isfile(path)=$(isfile(path))")
    if !((loc_name == "Remote" && (occursin(".h5", loc_params["path"]) || occursin(".hdf5", loc_params["path"]))) ||
        (loc_name == "Disk" && HDF5.ishdf5(path)))
        error("Expected HDF5 file to read in; failed to read from $path")
    end

    println("In ReadBlockHDF5 with HDF5.ishdf5(path)=$(HDF5.ishdf5(path))")
       
    # @show isfile(path)
    f = h5open(path, "r")
    println("In ReadBlockHDF5 after h5open")
    dset = loc_name == "Disk" ? f["part"] : f[loc_params["subpath"]]

    ismapping = false
    # TODO: Use `view` instead of `getindex` in the call to
    # `Banyan.split_on_executor` here if HDF5 doesn't support this kind of usage
    # TODO: Support modifying a memory-mappable file here without having
    # to read and then write back
    # if ismmappable(dset)
    #     ismapping = true
    #     dset = readmmap(dset)
    #     close(f)
    #     dset = Banyan.split_on_executor(dset, params["key"], batch_idx, nbatches, comm)
    # else
    dim = params["key"]
    dimsize = size(dset, dim)
    dimrange = Banyan.split_len(dimsize, batch_idx, nbatches, comm)
    dset = if length(dimrange) == 0
        # If we want to read in an emoty dataset, it's a little tricky to
        # do that with HDF5.jl. But this is how we do it:
        if dimsize == 0
            dset[[Colon() for _ in 1:ndims(dset)]...]
        else
            dset[[
                # We first read in the first slice into memory. This is
                # because HDF5.jl (unlike h5py) does not support just
                # reading in an empty `1:0` slice.
                if i == dim
                    1:1
                else
                    Colon()
                end for i = 1:ndims(dset)
            ]...][[
                # Then once that row is in memory we just remove it so
                # that we have the appropriate empty slice.
                if i == dim
                    1:0
                else
                    Colon()
                end for i = 1:ndims(dset)
            ]...]
        end
    else 
        # If it's not an empty slice that we want to read, it's pretty
        # straightforward - we just specify the slice.
        dset[[
            if i == dim
                dimrange
            else
                Colon()
            end for i = 1:ndims(dset)
        ]...]
    end
    close(f)
    println("In ReadBlockHDF5 at end")
    dset
end

ReadGroupHDF5 = Banyan.ReadGroup(ReadBlockHDF5)

function WriteHDF5(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # Get rid of splitting divisions if they were used to split this data into
    # groups
    splitting_divisions = Banyan.get_splitting_divisions()
    delete!(splitting_divisions, part)

    # Get path of directory to write to
    path = loc_params["path"]
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = Banyan.getpath(path)
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else
        # Prepend "efs/" for local paths
        path = Banyan.getpath(path)
    end

    worker_idx = Banyan.get_worker_idx(comm)
    idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)

    # TODO: Use Julia serialization to write arrays as well as other
    # objects to disk. This way, we won't trip up when we come across
    # a distributed array that we want to write to disk but can't because
    # of an unsupported HDF5 data type.
    # TODO: Support missing values in the array for locations that use
    # Julia serialized objects
    part = Missings.disallowmissing(part)

    if !hasmethod(HDF5.datatype, (eltype(part),))
        error("Unable to write array with element type $(eltype(part)) to HDF5 dataset at $(loc_params["path"])")
    end

    dim = params["key"]
    # TODO: Ensure that wherever we are using MPI for reduction or
    # broadcasts, we should always ensure that we cast back into
    # the original data type. Even if we have an isbits type like a tuple,
    # we will still want to cast to a tuple
    # TODO: Ensure this works where some partitions are empty
    # If each worker has a single batch, compute the total size
    # TODO: Compute exscan of size across the nodes, on the last node,
    # create the dataset, apply a barrier, and then synchronize so that
    # we can have each worker write on the range from its scanned value
    # with the size of data

    # TODO: Check if HDF5 dataset is created. If not, wait for the master
    # node to create it.
    # If each worker has a single batch, create a file on the first worker.
    # Otherwise, if we need multiple batches for each partition, write each
    # batch to a separate group. TODO: If we need multiple batches, don't
    # write the last batch to its own group. Instead just write it into the
    # aggregated group.
    group_prefix = loc_name == "Disk" ? "part" : loc_params["subpath"]
    partition_idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
    worker_idx = Banyan.get_worker_idx(comm)
    nworkers = Banyan.get_nworkers(comm)
    group = nbatches == 1 ? group_prefix : group_prefix * "_part$idx" * "_dim=$dim"

    # TODO: Have an option in the location to set this to either "w" or
    # "cw". Both will create a new file if it's not already there but
    # "w" will delete all existing datasets while "cw" will keep all
    # existing datasets (and fail if you try to write over anything). We
    # should also maybe have an option for "cw" but if you try to write
    # to an existing dataset, we will simply delete that dataset first.
    # dataset_writing_permission = "cw"
    force_overwrite = true

    info = MPI.Info()

    # Write out to an HDF5 dataset differently depending on whether there
    # are multiple batches per worker or just one per worker
    if nbatches == 1
        # Determine the offset into the resulting HDF5 dataset where this
        # worker should write
        offset = MPI.Exscan(size(part, dim), +, comm)
        if worker_idx == 1
            offset = 0
        end

        # Create file if not yet created
        # TODO: Figure out why sometimes a deleted file still `isfile`
        f = h5open(
            path,
            "cw",
            fapl_mpio = (comm, info),
            dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
        )
        close(f)

        MPI.Barrier(comm)

        # Open file for writing data
        f = h5open(path, "r+", comm, info)

        # Overwrite existing dataset if found
        # TODO: Return error on client side if we don't want to allow this
        if force_overwrite && haskey(f, group)
            delete_object(f[group])
        end

        # Create dataset
        whole_size = Banyan.indexapply(+, size(part), offset, index = dim)
        whole_size = MPI.bcast(whole_size, nworkers - 1, comm) # Broadcast dataset size to all workers
        dset = create_dataset(f, group, eltype(part), (whole_size, whole_size))

        # Write out each partition
        setindex!(
            dset,
            part,
            [
                # d == dim ? Banyan.split_len(whole_size[dim], batch_idx, nbatches, comm) :
                if d == dim
                    (offset+1):(offset+size(part, dim))
                else
                    Colon()
                end for d = 1:ndims(dset)
            ]...,
        )

        # Close file
        close(dset)
        close(f)
        # Not needed since we barrier at the end of each iteration of a merging
        # stage with I/O
        # MPI.Barrier(comm)
    else
        # TODO: See if we have missing `close`s or missing `fsync`s or extra `MPI.Barrier`s
        # fsync_file(p) =
        #     open(p) do f
        #         # TODO: Maybe use MPI I/O method for fsync instead
        #         ccall(:fsync, Cint, (Cint,), fd(f))
        #     end

        # Create the file if not yet created
        if batch_idx == 1
            f = h5open(
                path,
                "cw",
                fapl_mpio = (comm, info),
                dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
            )
            close(f)
            MPI.Barrier(comm)
        end

        # TODO: Maybe use fsync to flush out after the file is closed
        # TODO: Use a separate file for intermediate datasets so that we
        # don't create a bunch of extra datasets everytime we try to write

        # TODO: Maybe pass in values for fapl_mpi and
        # dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
        f = h5open(path, "r+", comm, info)
        # Allocate all datasets needed by gathering all sizes to the head
        # node and making calls from there
        part_lengths = MPI.Allgather(size(part, dim), comm)

        partdsets = [
            begin
                idx = Banyan.get_partition_idx(batch_idx, nbatches, worker_i)
                group = group_prefix * "_part$idx" * "_dim=$dim"
                # If there are multiple batches, each batch just gets written
                # to its own group
                dataspace_size =
                    Banyan.indexapply(_ -> part_length, size(part), index = dim)
                # TODO: Maybe pass in values for fapl_mpi and
                # dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
                new_dset = create_dataset(
                    f,
                    group,
                    eltype(part),
                    (dataspace_size, dataspace_size),
                )
                new_dset
            end for (worker_i, part_length) in enumerate(part_lengths)
        ]

        # Wait for the head node to allocate all the datasets for this
        # batch index
        # TODO: Ensure that nothing doesn't cause bcast to just become a
        # no-op or something like that
        # MPI.bcast(nothing, 0, comm)
        # TODO: Try removing this barrier
        MPI.Barrier(comm)

        # Each worker then writes their partition to a separate dataset
        # in parallel
        partdsets[worker_idx][Base.fill(Colon(), ndims(part))...] = part

        # Close (flush) all the intermediate datasets that we have created
        # TODO: Try removing this barrier
        MPI.Barrier(comm)
        for partdset in partdsets
            close(partdset)
        end
        # TODO: Try removing this barrier
        MPI.Barrier(comm)

        # Collect datasets from each batch and write into the final result dataset
        if batch_idx == nbatches
            # Get all intermediate datasets that have been written to by this worker
            partdsets = [
                begin
                    # Determine what index partition this batch is
                    idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)

                    # Get the dataset
                    group = group_prefix * "_part$idx" * "_dim=$dim"
                    f[group]
                end for batch_idx = 1:nbatches
            ]

            # Compute the size of all the batches on this worker
            # concatenated
            whole_batch_length = sum([size(partdset, dim) for partdset in partdsets])

            # Determine the offset into the resulting HDF5 dataset where this
            # worker should write
            offset = MPI.Exscan(whole_batch_length, +, comm)
            if worker_idx == 1
                offset = 0
            end

            # Make the last worker create the dataset (since it can compute
            # the total dataset size using its offset)
            # NOTE: It's important that we use the last node since the
            # last node has the scan result
            whole_size =
                Banyan.indexapply(_ -> offset + whole_batch_length, size(part), index = dim)
            whole_size = MPI.bcast(whole_size, nworkers - 1, comm) # Broadcast dataset size to all workers
            # The permission used here is "r+" because we already
            # created the file on the head node
            # Delete the dataset if needed before we write the
            # dataset to which each batch will write its chunk
            if force_overwrite && haskey(f, group_prefix)
                delete_object(f[group_prefix])
            end

            # If there are multiple batches, each batch just gets written
            # to its own group
            dset =
                create_dataset(f, group_prefix, eltype(part), (whole_size, whole_size))

            # Wait until all workers have the file
            # TODO: Maybe use a broadcast so that each node is only blocked on
            # the last node which is where the file is creating
            # TODO: Try removing this barrier
            MPI.Barrier(comm)

            # Write out each batch
            batchoffset = offset
            for batch_i = 1:nbatches
                partdset = partdsets[batch_i]

                # Determine what index partition this batch is
                idx = Banyan.get_partition_idx(batch_i, nbatches, comm)

                # Write
                group = group_prefix * "_part$idx" * "_dim=$dim"
                partdset_reading = partdset[Base.fill(Colon(), ndims(dset))...]

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after reading batch $batch_i with available memory: $(Banyan.format_available_memory())")
                setindex!(
                    # We are writing to the whole dataset that was just
                    # created
                    dset,
                    # We are copying from the written HDF5 dataset for a
                    # particular batch
                    partdset_reading,
                    # We write to the appropriate split of the whole
                    # dataset
                    [
                        if d == dim
                            (batchoffset+1):batchoffset+size(partdset, dim)
                        else
                            Colon()
                        end
                        # Banyan.split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
                        for d = 1:ndims(dset)
                    ]...,
                )
                partdset_reading = nothing

                # Update the offset of this batch
                batchoffset += size(partdset, dim)
                close(partdset)
                partdset = nothing
            end
            close(dset)
            dset = nothing
            # fsync_file()

            # TODO: Delete data by keeping intermediates in separate file
            # Wait until all the data is written
            # TODO: Try removing this barrier
            MPI.Barrier(comm)
            # NOTE: Issue is that the barrier here doesn't ensure that all
            # processes have written in the previous step
            # TODO: Use a broadcast here

            # Then, delete all data for all groups on the head node
            for worker_i = 1:nworkers
                for batch_i = 1:nbatches
                    idx = Banyan.get_partition_idx(batch_i, nbatches, worker_i)
                    group = group_prefix * "_part$idx" * "_dim=$dim"
                    delete_object(f[group])
                end
            end
            # TODO: Ensure that closing (flushing) HDF5 datasets
            # and files is sufficient. We might additionally have
            # to sync to ensure that the content is actually
            # written to disk or to S3
            # TODO: Use create_dataset passing in dtype and dimensions
            # fsync_file()

            # # TODO: Determine whether this is necessary. This barrier might
            # # be necessary to ensure that all groups are deleted before we
            # # continue.
            MPI.Barrier(comm)
        end
        close(f)
        f = nothing
        # TODO: Ensure that we are closing stuff everywhere before trying
        # to write

        if batch_idx < nbatches
            MPI.Barrier(comm)
        end
    end
end

CopyFromHDF5(src, params, batch_idx, nbatches, comm, loc_name, loc_params) = begin
    params["key"] = 1
    ReadBlockHDF5(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyToHDF5(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

function Banyan.SplitBlock(
    src::AbstractArray,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    Banyan.split_on_executor(
        src,
        params["key"],
        batch_idx,
        nbatches,
        comm,
    )
end

function Banyan.SplitGroup(
    src::AbstractArray,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
)

    partition_idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
    npartitions = get_npartitions(nbatches, comm)

    # Ensure that this partition has a schema that is suitable for usage
    # here. We have to do this for `Shuffle` and `SplitGroup` (which is
    # used by `DistributeAndShuffle`)
    if isempty(src) || npartitions == 1
        # TODO: Ensure we can return here like this and don't need the above
        # (which is copied from `Shuffle`)
        return src
    end

    # Get divisions stored with src
    splitting_divisions = Banyan.get_splitting_divisions()
    src_divisions, boundedlower, boundedupper = get!(splitting_divisions, src) do
        # This case lets us use `SplitGroup` in `DistributeAndShuffle`
        (params["divisions"], false, false)
    end
    divisions_by_partition = Banyan.get_divisions(src_divisions, npartitions)

    # Get the divisions to apply
    key = params["key"]
    rev = get(params, "rev", false)
    if rev
        reverse!(divisions_by_partition)
    end

    # Create helper function for getting index of partition that owns a given
    # value
    partition_idx_getter(val) = Banyan.get_partition_idx_from_divisions(
        val,
        divisions_by_partition,
        boundedlower = boundedlower,
        boundedupper = boundedupper,
    )

    # Apply divisions to get only the elements relevant to this worker
    res = if ndims(src) > 1
        cat(
            [
                slice
                for slice in eachslice(src, dims = key)
                if partition_idx_getter(slice) == partition_idx
            ]...;
            dims = key,
        )
    else
        filter(
            e -> partition_idx_getter(e) == partition_idx,
            src
        )
    end

    if store_splitting_divisions
        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any(x->!isempty(x), divisions_by_partition)
        firstdivisionidx = findfirst(x->!isempty(x), divisions_by_partition)
        lastdivisionidx = findlast(x->!isempty(x), divisions_by_partition)

        # Store divisions
        splitting_divisions = Banyan.get_splitting_divisions()
        splitting_divisions[res] = (
            divisions_by_partition[partition_idx],
            !hasdivision || boundedlower || partition_idx != firstdivisionidx,
            !hasdivision || boundedupper || partition_idx != lastdivisionidx,
        )
    end

    res
end

function Banyan.Rebalance(
    part::AbstractArray,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
)
    # Get the range owned by this worker
    dim = dst_params["key"]
    worker_idx, nworkers = Banyan.get_worker_idx(comm), Banyan.get_nworkers(comm)
    len = size(part, dim)
    scannedstartidx = MPI.Exscan(len, +, comm)
    startidx = worker_idx == 1 ? 1 : scannedstartidx + 1
    endidx = startidx + len - 1

    # Get functions for serializing/deserializing
    ser = serialize
    # TODO: Use JLD for ser/de for arrays
    # TODO: Ensure that we are properly handling intermediate arrays or
    # dataframes that are empty (especially because they may not have their
    # ndims or dtype or schema). We probably are because dataframes that are
    # empty should concatenate properly. We just need to be sure to not expect
    # every partition to know what its schema is. We can however expect each
    # partition of an array to know its ndims.
    de = x -> deserialize(IOBuffer(x))

    # NOTE: Below this is all common between Rebalance for DataFrame and AbstractArray

    # Construct buffer to send parts to all workers who own in this range
    nworkers = Banyan.get_nworkers(comm)
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Base.Vector{Int64} = []
    for partition_idx = 1:npartitions
        # `Banyan.split_len` gives us the range that this partition needs
        partitionrange = Banyan.split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap =
            max(startidx, partitionrange.start) <= min(endidx, partitionrange.stop)

        # If they do overlap, then serialize the overlapping slice
        ser(
            io,
            view(
                part,
                Base.fill(:, dim - 1)...,
                if rangesoverlap
                    max(1, partitionrange.start - startidx + 1):min(
                        size(part, dim),
                        partitionrange.stop - startidx + 1,
                    )
                else
                    # Return zero length for this dimension
                    1:0
                end,
                Base.fill(:, ndims(part) - dim)...,
            ),
        )

        # Add the count of the size of this chunk in bytes
        push!(counts, io.size - nbyteswritten)
        nbyteswritten = io.size

    end
    sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), counts)

    # Create buffer for receiving pieces
    # TODO: Refactor the intermediate part starting from there if we add
    # more cases for this function
    sizes = MPI.Alltoall(MPI.UBuffer(counts, 1), comm)
    recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

    # Perform the shuffle
    MPI.Alltoallv!(sendbuf, recvbuf, comm)

    # Return the concatenated array
    things_to_concatenate = [
        de(view(recvbuf.data, displ+1:displ+count)) for
        (displ, count) in zip(recvbuf.displs, recvbuf.counts)
    ]
    res = merge_on_executor(
        things_to_concatenate...;
        key = dim,
    )
    res
end

function Banyan.Consolidate(part::AbstractArray, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm)
    is_buffer_type = ndims(part) == 1 && isbitstype(eltype(part))
    sendbuf = if is_buffer_type
        MPI.Buffer(part)
    else
        io = IOBuffer()
        serialize(io, part)
        MPI.Buffer(view(io.data, 1:io.size))
    end
    recvvbuf = Banyan.buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized

    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    merge_on_executor(
        [
            begin
                chunk = view(recvvbuf.data, (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i]))
                is_buffer_type ? chunk : deserialize(IOBuffer(chunk))
            end
            for i in 1:Banyan.get_nworkers(comm)
        ]...;
        key = src_params["key"],
    )
end

function Banyan.Shuffle(
    part::AbstractArray,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm;
    boundedlower = false,
    boundedupper = false,
    store_splitting_divisions = true
)
    # We don't have to worry about grouped data frames since they are always
    # block-partitioned.

    # Get the divisions to apply
    key = dst_params["key"]
    rev = get(dst_params, "rev", false)
    worker_idx, nworkers = Banyan.get_worker_idx(comm), Banyan.get_nworkers(comm)
    divisions_by_worker = if haskey(dst_params, "divisions_by_worker")
        dst_params["divisions_by_worker"] # list of min-max tuples
    else 
        Banyan.get_divisions(dst_params["divisions"], nworkers)
    end # list of min-max tuple lists
    if rev
        reverse!(divisions_by_worker)
    end

    # Perform shuffle
    partition_idx_getter(val) = Banyan.get_partition_idx_from_divisions(
        val,
        divisions_by_worker,
        boundedlower = boundedlower,
        boundedupper = boundedupper,
    )
    res = begin
        # Group the data along the splitting axis (specified by the "key"
        # parameter)
        multidimensional = ndims(part) > 1
        if multidimensional
            partition_idx_to_e = [[] for partition_idx = 1:nworkers]
            for e in eachslice(part, dims = key)
                partition_idx = partition_idx_getter(e)
                if partition_idx != -1
                    push!(partition_idx_to_e[partition_idx], e)
                end
            end
        else
            part_sortperm = sortperm(part, by=partition_idx_getter)
            part_sortperm_idx = 1
        end

        # Construct buffer for sending data
        io = IOBuffer()
        nbyteswritten = 0
        a_counts::Base.Vector{Int64} = []
        for partition_idx = 1:nworkers
            if multidimensional
                # TODO: If `isbitstype(eltype(e))`, we may want to pass it in
                # directly as an MPI buffer (if there is such a thing) instead of
                # serializing
                # Append to serialized buffer
                e = partition_idx_to_e[partition_idx]
                # NOTE: We ensure that we serialize something (even if its an
                # empty array) for each partition to ensure that we can
                # deserialize each item
                serialize(
                    io,
                    !isempty(e) ? cat(e...; dims = key) :
                    view(part, [
                        if d == key
                            1:0
                        else
                            Colon()
                        end for d = 1:ndims(part)
                    ]...),
                )
            else
                next_part_sortperm_idx = part_sortperm_idx
                while partition_idx_getter(part_sortperm[part_sortperm_idx]) == partition_idx
                    next_part_sortperm_idx += 1
                end
                serialize(io, part[@view part_sortperm[part_sortperm_idx:next_part_sortperm_idx-1]])
            end

            # Add the count of the size of this chunk in bytes
            push!(a_counts, io.size - nbyteswritten)
            nbyteswritten = io.size
        end
        sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), a_counts)

        # Create buffer for receiving pieces
        # TODO: Refactor the intermediate part starting from there if we add
        # more cases for this function
        sizes = MPI.Alltoall(MPI.UBuffer(a_counts, 1), comm)
        recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

        # Perform the shuffle
        MPI.Alltoallv!(sendbuf, recvbuf, comm)

        # Return the concatenated array
        things_to_concatenate = [
            deserialize(IOBuffer(view(recvbuf.data, displ+1:displ+count))) for
            (displ, count) in zip(recvbuf.displs, recvbuf.counts)
        ]
        if length(things_to_concatenate) == 1
            things_to_concatenate[1]
        else
            cat(
                things_to_concatenate...;
                dims = key,
            )
        end
    end

    if store_splitting_divisions
        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any(x->!isempty(x), divisions_by_worker)
        firstdivisionidx = findfirst(x->!isempty(x), divisions_by_worker)
        lastdivisionidx = findlast(x->!isempty(x), divisions_by_worker)

        # Store divisions
        splitting_divisions = Banyan.get_splitting_divisions()
        splitting_divisions[res] =
            (divisions_by_worker[worker_idx], !hasdivision || worker_idx != firstdivisionidx, !hasdivision || worker_idx != lastdivisionidx)
    end

    res
end