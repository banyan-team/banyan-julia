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
    path = Banyan.getpath(loc_params["path"], comm)
    if isinvestigating()[:parallel_hdf5]
        println("In ReadBlockHDF5 with path=$path, loc_name=$loc_name, isfile(path)=$(isfile(path))")
    end
    if !((loc_name == "Remote" && (occursin(".h5", loc_params["path"]) || occursin(".hdf5", loc_params["path"]))) ||
        (loc_name == "Disk" && HDF5.ishdf5(path)))
        error("Expected HDF5 file to read in; failed to read from $path")
    end

    if isinvestigating()[:parallel_hdf5]
        println("In ReadBlockHDF5 with HDF5.ishdf5(path)=$(HDF5.ishdf5(path))")
    end
       
    # @show isfile(path)
    f = h5open(path, "r")
    if isinvestigating()[:parallel_hdf5]
        println("In ReadBlockHDF5 after h5open")
    end
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
    if isinvestigating()[:parallel_hdf5]
        println("In ReadBlockHDF5 at end with size(dset)=$(size(dset)), dimrange=$dimrange")
    end
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
        path = Banyan.getpath(path, comm)
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else
        # Prepend "efs/" for local paths
        path = Banyan.getpath(path, comm)
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

function CopyToHDF5(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
        params["key"] = 1
        WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
end