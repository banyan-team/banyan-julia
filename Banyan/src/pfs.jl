# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pt_lib.jl` should have a corresponding
# `pt_lib_info.json` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

###################################
# Splitting and merging functions #
###################################

ReturnNull(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = nothing

format_available_memory() =
    format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

function sortablestring(val, maxval)
    s = string(val)
    maxs = string(maxval)
    res = fill('0', length(maxs))
    res[length(res)-length(s)+1:length(res)] .= collect(s)
    join(res)
end

function ReadBlockHDF5()
    # Handle single-file nd-arrays
    # We check if it's a file because for items on disk, files are HDF5
    # datasets while directories contain Parquet, CSV, or Arrow datasets
    path = getpath(loc_params["path"])
    if !((loc_name == "Remote" && (occursin(".h5", loc_params["path"]) || occursin(".hdf5", loc_params["path"]))) ||
        (loc_name == "Disk" && HDF5.ishdf5(path)))
        error("Expected HDF5 file to read in")
    end
       
    # @show isfile(path)
    f = h5open(path, "r")
    dset = loc_name == "Disk" ? f["part"] : f[loc_params["subpath"]]

    ismapping = false
    # TODO: Use `view` instead of `getindex` in the call to
    # `split_on_executor` here if HDF5 doesn't support this kind of usage
    # TODO: Support modifying a memory-mappable file here without having
    # to read and then write back
    # if ismmappable(dset)
    #     ismapping = true
    #     dset = readmmap(dset)
    #     close(f)
    #     dset = split_on_executor(dset, params["key"], batch_idx, nbatches, comm)
    # else
    dim = params["key"]
    dimsize = size(dset, dim)
    dimrange = split_len(dimsize, batch_idx, nbatches, comm)
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
    dset
end

function write_csv_file(path, header, readrange, filerowrange, dfs)
    f = CSV.File(
        path,
        header = header,
        skipto = header + readrange.start - filerowrange.start + 1,
        footerskip = filerowrange.stop - readrange.stop,
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

function write_parquet_file(path, header, readrange, filerowrange, dfs)
    f = Parquet.read_parquet(
        path,
        rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

function write_arrow_file(path, header, readrange, filerowrange, dfs)
    rbrowrange = filerowrange.start:(filerowrange.start-1)
    for tbl in Arrow.Stream(path)
        rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+Tables.rowcount(tbl))
        if isoverlapping(rbrowrange, rowrange)
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

ReadBlockCSV, ReadBlockParquet, ReadBlockArrow = [
    begin
        function ReadBlock(
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

            path = getpath(loc_params["path"])

            # Handle multi-file tabular datasets

            # Handle None location by finding all files in directory used for spilling
            # this value to disk
            if loc_name == "Disk"
                # TODO: Only collect files and nrows info for this location associated
                # with a unique name corresponding to the value ID - only if this is
                # the first batch or loop iteration.
                name = loc_params["path"]
                name_path = getpath(name)
                # TODO: isdir might not work for S3FS
                if isdir(name_path)
                    files = []
                    nrows = 0
                    for partfilename in readdir(name_path)
                        part_nrows = parse(
                            Int64,
                            replace(split(partfilename, "_nrows=")[end], ".arrow" => ""),
                        )
                        push!(
                            files,
                            Dict("nrows" => part_nrows, "path" => joinpath(name, partfilename)),
                        )
                        nrows += part_nrows
                    end
                    loc_params["files"] = files
                    loc_params["nrows"] = nrows
                else
                    # This is the case where no data has been spilled to disk and this
                    # is maybe just an intermediate variable only used for this stage.
                    # We never spill tabular data to a single file - it's always a
                    # directory of Arrow files.
                    return nothing
                end
            end

            # Iterate through files and identify which ones correspond to the range of
            # rows for the batch currently being processed by this worker
            nrows = loc_params["nrows"]
            rowrange = split_len(nrows, batch_idx, nbatches, comm)
            dfs::Vector{DataFrames.DataFrame} = []
            rowsscanned = 0
            for file in sort(loc_params["files"], by = filedict -> filedict["path"])
                newrowsscanned = rowsscanned + file["nrows"]
                filerowrange = (rowsscanned+1):newrowsscanned
                # Check if the file corresponds to the range of rows for the batch
                # currently being processed by this worker
                if isoverlapping(filerowrange, rowrange)
                    # Deterine path to read from
                    file_path = file["path"]
                    path = getpath(file_path)

                    # Read from location depending on data format
                    readrange =
                        max(rowrange.start, filerowrange.start):min(
                            rowrange.stop,
                            filerowrange.stop,
                        )
                    header = 1
                    # TODO: Scale the memory usage appropriately when splitting with
                    # this and garbage collect if too much memory is used.
                    if endswith(file_path, file_extension)
                        write_file(path, header, readrange, filerowrange, dfs)
                    else
                        error("Expected file with $file_extension extension")
                    end
                end
                rowsscanned = newrowsscanned
            end

            # Concatenate and return
            # NOTE: If this partition is empty, it is possible that the result is
            # schemaless (unlike the case with HDF5 where the resulting array is
            # guaranteed to have its ndims correct) and so if a split/merge/cast
            # function requires the schema (for example for grouping) then it must be
            # sure to take that account
            if isempty(dfs)
                # Note that if we are reading disk-spilled Arrow data, we would have
                # files for each of the workers that wrote that data. So there should
                # be files but they might be empty.
                if loc_name == "Disk"
                    files_sorted_by_nrow = sort(loc_params["files"], by = filedict -> filedict["nrows"])
                    if isempty(files_sorted_by_nrow)
                        # This should not be empty for disk-spilled data
                        DataFrame()
                    else
                        empty(DataFrames.DataFrame(Arrow.Table(getpath(first(files_sorted_by_nrow)["path"])), copycols=false))
                    end
                else
                    # When we construct the location, we store an empty data frame with The
                    # correct schema.
                    from_jl_value_contents(loc_params["emptysample"])
                end
            elseif length(dfs) == 1
                dfs[1]
            else
                vcat(dfs...)
            end
        end
        ReadBlock
    end
    for read_file, file_extension in [
        (read_csv_file, ".csv"),
        (read_parquet_file, ".parquet"),
        (read_arrow_file, ".arrow")
    ]
]

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
    rev = params["rev"] # Passed in ReadBlock
    nworkers = get_nworkers(comm)
    npartitions = nworkers * nbatches
    partition_divisions = get_divisions(divisions, npartitions)

    # TODO: Do some reversing here instead of only doing it later in Shuffle
    # to ensure that sorting in reverse order works correctly

    # The first and last partitions (used if this lacks a lower or upper bound)
    # must have actual division(s) associated with them. If there is no
    # partition that has divisions, then they will all be skipped and -1 will
    # be returned. So these indices are only used if there are nonempty
    # divisions.
    hasdivision = any(x->!isempty(x), partition_divisions)
    firstdivisionidx = findfirst(x->!isempty(x), partition_divisions)
    lastdivisionidx = findlast(x->!isempty(x), partition_divisions)
    firstbatchidx = nothing
    lastbatchidx = nothing

    # Get the divisions that are relevant to this batch by iterating
    # through the divisions in a stride and consolidating the list of divisions
    # for each partition. Then, ensure we use boundedlower=true only for the
    # first batch and boundedupper=true for the last batch.
    curr_partition_divisions = []
    for worker_division_idx = 1:nworkers
        for batch_division_idx = 1:nbatches
            # partition_division_idx =
            #     (worker_division_idx - 1) * nbatches + batch_division_idx
            partition_division_idx =
                get_partition_idx(batch_division_idx, nbatches, worker_division_idx)
            if batch_division_idx == batch_idx
                # Get the divisions for this partition
                p_divisions = partition_divisions[partition_division_idx]

                # We've already used `get_divisions` to get a list of min-max
                # tuples (we call these tuples "divisions") for each partition
                # that `ReadGroup` produces. But we only want to read in all
                # the partitions relevant for this batch. But it is important
                # then that `curr_partition_divisions` has an element for each
                # worker. That way, when we call `Shuffle`, it will properly
                # read data onto each worker that is in the appropriate
                # partition.
                push!(
                    curr_partition_divisions,
                    p_divisions,
                )
            end

            # Find the batches that have the first and last divisions
            if partition_division_idx == firstdivisionidx
                firstbatchidx = batch_division_idx
            end
            if partition_division_idx == lastdivisionidx
                lastbatchidx = batch_division_idx
            end
        end
    end

    # Read in each batch and shuffle it to get the data for this partition
    parts = []
    for i = 1:nbatches
        # Read in data for this batch
        part = ReadBlock(src, params, i, nbatches, comm, loc_name, loc_params)

        # Shuffle the batch and add it to the set of data for this partition
        params["divisions_by_worker"] = curr_partition_divisions
        push!(
            parts,
            Shuffle(
                part,
                Dict{String,Any}(),
                params,
                comm,
                boundedlower = !hasdivision || batch_idx != firstbatchidx,
                boundedupper = !hasdivision || batch_idx != lastbatchidx,
                store_splitting_divisions = false
            ),
        )
        delete!(params, "divisions_by_worker")
    end

    # Concatenate together the data for this partition
    res = merge_on_executor(parts...; key = key)

    # If there are no divisions for any of the partitions, then they are all
    # bounded. For a partition to be unbounded on one side, there must be a
    # division(s) for that partition.

    # Store divisions
    global splitting_divisions
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    splitting_divisions[res] =
        (partition_divisions[partition_idx], !hasdivision || partition_idx != firstdivisionidx, !hasdivision || partition_idx != lastdivisionidx)

    res
end

function rmdir_on_nfs(actualpath)
    if isdir(actualpath)
        for actualpath_f in readdir(actualpath, join=true)
            if isfile(actualpath_f)
                rm(actualpath_f, force=true)
            end
        end
    end
    # TODO: Also try to remove the directory itself right away although there
    # might still be .nfs files in it. This isn't too much of a problem since we _do_ try
    # to remove all directories at the end of the job.
end

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
    global splitting_divisions
    delete!(splitting_divisions, part)

    # Get path of directory to write to
    path = loc_params["path"]
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = getpath(path)
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else
        # Prepend "efs/" for local paths
        path = getpath(path)
    end

    worker_idx = get_worker_idx(comm)
    idx = get_partition_idx(batch_idx, nbatches, comm)

    if !hasmethod(HDF5.datatype, (eltype(part),))
        error("Unable to write array with element type $(eltype(part)) to HDF5 dataset at $(loc_params["path"])")
    end

    # TODO: Use Julia serialization to write arrays as well as other
    # objects to disk. This way, we won't trip up when we come across
    # a distributed array that we want to write to disk but can't because
    # of an unsupported HDF5 data type.
    # TODO: Support missing values in the array for locations that use
    # Julia serialized objects
    part = DataFrames.disallowmissing(part)

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
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    worker_idx = get_worker_idx(comm)
    nworkers = get_nworkers(comm)
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
        whole_size = indexapply(+, size(part), offset, index = dim)
        whole_size = MPI.bcast(whole_size, nworkers - 1, comm) # Broadcast dataset size to all workers
        dset = create_dataset(f, group, eltype(part), (whole_size, whole_size))

        # Write out each partition
        setindex!(
            dset,
            part,
            [
                # d == dim ? split_len(whole_size[dim], batch_idx, nbatches, comm) :
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
                idx = get_partition_idx(batch_idx, nbatches, worker_i)
                group = group_prefix * "_part$idx" * "_dim=$dim"
                # If there are multiple batches, each batch just gets written
                # to its own group
                dataspace_size =
                    indexapply(_ -> part_length, size(part), index = dim)
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
        partdsets[worker_idx][fill(Colon(), ndims(part))...] = part

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
                    idx = get_partition_idx(batch_idx, nbatches, comm)

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
                indexapply(_ -> offset + whole_batch_length, size(part), index = dim)
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
                idx = get_partition_idx(batch_i, nbatches, comm)

                # Write
                group = group_prefix * "_part$idx" * "_dim=$dim"
                partdset_reading = partdset[fill(Colon(), ndims(dset))...]

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after reading batch $batch_i with available memory: $(format_available_memory())")
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
                        # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
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
                    idx = get_partition_idx(batch_i, nbatches, worker_i)
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

write_parquet_file(part, path, sortableidx, nrows) = if nrows > 0
    Parquet.write_parquet(joinpath(path, "part$sortableidx" * "_nrows=$nrows.parquet"), part)
end

write_csv_file(part, path, sortableidx, nrows) = CSV.write(
    joinpath(path, "part$sortableidx" * "_nrows=$nrows.csv"),
    part
)

write_arrow_file(part, path, sortableidx, nrows) = Arrow.write(
    joinpath(path, "part$sortableidx" * "_nrows=$nrows.arrow"),
    part
)

WriteParquet, WriteCSV, WriteArrow = [
    begin
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
            # Get rid of splitting divisions if they were used to split this data into
            # groups
            global splitting_divisions
            delete!(splitting_divisions, part)

            # Get path of directory to write to
            path = loc_params["path"]
            if startswith(path, "http://") || startswith(path, "https://")
                error("Writing to http(s):// is not supported")
            elseif startswith(path, "s3://")
                path = getpath(path)
                # NOTE: We expect that the ParallelCluster instance was set up
                # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
            else
                # Prepend "efs/" for local paths
                path = getpath(path)
            end

            # Write file for this partition
            worker_idx = get_worker_idx(comm)
            idx = get_partition_idx(batch_idx, nbatches, comm)
            if isa_df(part)
                actualpath = deepcopy(path)
                if nbatches > 1
                    # Add _tmp to the end of the path
                    if endswith(path, ".parquet")
                        path = replace(path, ".parquet" => "_tmp.parquet")
                    elseif endswith(path, ".csv")
                        path = replace(path, ".csv" => "_tmp.csv")
                    elseif endswith(path, ".arrow")
                        path = replace(path, ".arrow" => "_tmp.arrow")
                    else
                        path = path * "_tmp"
                    end
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
                        rmdir_on_nfs(actualpath)
                    end

                    # Create directory if it doesn't exist
                    # TODO: Avoid this and other filesystem operations that would be costly
                    # since S3FS is being used
                    if batch_idx == 1
                        rmdir_on_nfs(path)
                        mkpath(path)
                    end
                end
                MPI.Barrier(comm)

                nrows = size(part, 1)
                sortableidx = sortablestring(idx, get_npartitions(nbatches, comm))
                write_file(part, path, sortableidx, nrows)
                MPI.Barrier(comm)
                if nbatches > 1 && batch_idx == nbatches
                    tmpdir = readdir(path)
                    if worker_idx == 1
                        rmdir_on_nfs(actualpath)
                        mkpath(actualpath)
                    end
                    MPI.Barrier(comm)
                    for batch_i = 1:nbatches
                        idx = get_partition_idx(batch_i, nbatches, worker_idx)
                        tmpdir_idx = findfirst(fn -> startswith(fn, "part$idx"), tmpdir)
                        if !isnothing(tmpdir_idx)
                            tmpsrc = joinpath(path, tmpdir[tmpdir_idx])
                            actualdst = joinpath(actualpath, tmpdir[tmpdir_idx])
                            cp(tmpsrc, actualdst, force=true)
                        end
                    end
                    MPI.Barrier(comm)
                    if worker_idx == 1
                        rmdir_on_nfs(path)
                    end
                    MPI.Barrier(comm)
                end
                src
                # TODO: Delete all other part* files for this value if others exist
            end
        end
        Write
    end
    for write_file in [write_parquet_file, write_csv_file, write_arrow_file]
]

mutable struct PartiallyMerged
    pieces::Vector{Any}
end

SplitBlock(
    src::Nothing,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

SplitBlock(
    src::PartiallyMerged,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

function SplitBlock(
    src::AbstractArray,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    split_on_executor(
        src,
        params["key"],
        batch_idx,
        nbatches,
        comm,
    )
end

function SplitBlock(
    src::T,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    split_on_executor(
        src,
        1,
        batch_idx,
        nbatches,
        comm,
    )
end

# NOTE: The way we have `partial_merges` requires us to be splitting from
# `nothing` and then merging back. If we are splitting from some value and
# then expecting to merge back in some way then that won't work. If we are
# splitting from a value we assume that we don't have to merge back either
# because we split with a view (so the source was directly mutated) or we
# didn't mutate this value at all. If we are doing in-place mutations where
# we split from some value and then merge back up, then we might have to
# add support for that. Right now, because of the way `SplitBlock`,
# `SplitGroup`, and `Merge` are implemented, we unnecessarily concatenate
# in the case where we are doing things like `setindex!` with a somewhat
# faked mutation.

# src is [] if we are partially merged (because as we iterate over
# batches we take turns between splitting and merging)
SplitGroup(
    src::Union{Nothing,PartiallyMerged},
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
) = nothing
    

function SplitGroup(
    src::DataFrame,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
)

    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
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
    global splitting_divisions
    src_divisions, boundedlower, boundedupper = get!(splitting_divisions, src) do
        # This case lets us use `SplitGroup` in `DistributeAndShuffle`
        (params["divisions"], false, false)
    end
    divisions_by_partition = get_divisions(src_divisions, npartitions)

    # Get the divisions to apply
    key = params["key"]
    rev = params["rev"]
    if rev
        reverse!(divisions_by_partition)
    end

    # Create helper function for getting index of partition that owns a given
    # value
    partition_idx_getter(val) = get_partition_idx_from_divisions(
        val,
        divisions_by_partition,
        boundedlower = boundedlower,
        boundedupper = boundedupper,
    )

    # Apply divisions to get only the elements relevant to this worker
    # TODO: Do the groupby and filter on batch_idx == 1 and then share
    # among other batches
    res = filter(row -> partition_idx_getter(row[key]) == partition_idx, src)

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
        global splitting_divisions
        splitting_divisions[res] = (
            divisions_by_partition[partition_idx],
            !hasdivision || boundedlower || partition_idx != firstdivisionidx,
            !hasdivision || boundedupper || partition_idx != lastdivisionidx,
        )
    end

    res
end
    

function SplitGroup(
    src::AbstractArray,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
)

    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
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
    global splitting_divisions
    src_divisions, boundedlower, boundedupper = get!(splitting_divisions, src) do
        # This case lets us use `SplitGroup` in `DistributeAndShuffle`
        (params["divisions"], false, false)
    end
    divisions_by_partition = get_divisions(src_divisions, npartitions)

    # Get the divisions to apply
    key = params["key"]
    rev = params["rev"]
    if rev
        reverse!(divisions_by_partition)
    end

    # Create helper function for getting index of partition that owns a given
    # value
    partition_idx_getter(val) = get_partition_idx_from_divisions(
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
        global splitting_divisions
        splitting_divisions[res] = (
            divisions_by_partition[partition_idx],
            !hasdivision || boundedlower || partition_idx != firstdivisionidx,
            !hasdivision || boundedupper || partition_idx != lastdivisionidx,
        )
    end

    res
end

function Merge(
    src::Union{Nothing,PartiallyMerged},
    part,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    # TODO: Ensure we can merge grouped dataframes if computing them

    global splitting_divisions

    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
    # We only need to concatenate partitions if the source is nothing.
    # Because if the source is something, then part must be a view into it
    # and no data movement is needed.

    key = params["key"]

    # Concatenate across batches
    if batch_idx == 1
        src = PartiallyMerged(Vector{Any}(undef, nbatches))
    end
    src.pieces[batch_idx] = part
    if batch_idx == nbatches
        delete!(splitting_divisions, part)

        # Concatenate across batches
        src = merge_on_executor(src.pieces...; key = key)

        # Concatenate across workers
        nworkers = get_nworkers(comm)
        if nworkers > 1
            src = Consolidate(src, params, Dict{String,Any}(), comm)
        end
    end

    src
end

Merge(
    src,
    part,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = src

CopyFrom(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = src

CopyFromValue(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = loc_params["value"]

CopyFromClient(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    received = get_worker_idx(comm) == 1 ? receive_from_client(loc_params["value_id"]) : nothing
    # TODO: Make Replicated not necessarily require it to be replicated _everywhere_
    received = MPI.bcast(received, 0, comm)
    received
end

CopyFromJulia(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = isfile(getpath(loc_params["path"])) ? deserialize(path) : nothing

CopyFromHDF5(src, params, batch_idx, nbatches, comm, loc_name, loc_param,) = begin
    params["key"] = 1
    ReadBlockHDF5(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)s
end

CopyFromArrow(src, params, batch_idx, nbatches, comm, loc_name, loc_param,) = begin
    params["key"] = 1
    ReadBlockArrow(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)s
end

CopyFromCSV(src, params, batch_idx, nbatches, comm, loc_name, loc_param,) = begin
    params["key"] = 1
    ReadBlockCSV(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)s
end

CopyFromParquet(src, params, batch_idx, nbatches, comm, loc_name, loc_param,) = begin
    params["key"] = 1
    ReadBlockParquet(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)s
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
    src = part
end

CopyToClient(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) =  if get_partition_idx(batch_idx, nbatches, comm) == 1
    send_to_client(loc_params["value_id"], part)
end

CopyToJulia(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    # # This must be on disk; we don't support Julia serialized objects
    # # as a remote location yet. We will need to first refactor locations
    # # before we add support for that.
    # if isa_gdf(part)
    #     part = nothing
    # end
    serialize(path, part)
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
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyToCSV(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyToParquet(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyToArrow(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteHDF5(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

function get_op!(params::Dict{String,Any})
    op = params["reducer"]
    if params["with_key"]
        key = params["key"]
        if !haskey(params, "reducer_with_key")
            op = op(key)
            reducer_with_key = Dict(key => op)
            params["reducer_with_key"] = reducer_with_key
        else
            reducer_with_key = params["reducer_with_key"]
            if !haskey(reducer_with_key, key)
                op = op(key)
                reducer_with_key[key] = op
            else
                op = reducer_with_key[key]
            end
        end
    end
    op
end

reduce_in_memory(src::Nothing, part::T, op::Function) where {T} = part
reduce_in_memory(src, part::T, op::Function) where {T} = op(src, part)

function ReduceAndCopyToJulia(
    src,
    part::T,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) where {T}
    # Merge reductions from batches
    op = get_op!(params)
    # TODO: Ensure that we handle reductions that can produce nothing
    src = reduce_in_memory(src, part, op)

    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, params, Dict{String,Any}(), comm)

        if loc_name != "Memory"
            # We use 1 here so that it is as if we are copying from the head
            # node
            CopyToJulia(src, src, params, 1, nbatches, comm, loc_name, loc_params)
        end
    end

    # TODO: Ensure we don't have issues where with batched execution we are
    # merging to the thing we are splitting from
    # NOTE: We are probably okay for now because we never split and then
    # re-merge new results to the same variable. We always merge to a new
    # variable. But in the future to be more robust, we may want to store
    # partial merges in a global `IdDict` and then only mutate `src` once we
    # are finished with the last batch and we know we won't be splitting
    # from the value again.
    src
end

ReduceWithKeyAndCopyToJulia = ReduceAndCopyToJulia

function Divide(
    src::Tuple,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    dim = params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    newpartdim = length(split_len(part[dim], batch_idx, nbatches, comm))
    indexapply(_ -> newpartdim, part, index = dim)
end

function Divide(
    src,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    dim = params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    length(split_len(part[dim], batch_idx, nbatches, comm))
end

#####################
# Casting functions #
#####################

function Reduce(
    part::T,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) where {T}
    # Get operator for reduction
    op = get_op!(src_params)

    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

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

# Grouped data frames can be block-partitioned but we will have to
# redo the groupby if we try to do any sort of merging/splitting on it.
Rebalance(
    part::Union{Nothing,GroupedDataFrame},
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) = nothing

function Rebalance(
    part::AbstractArray,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
)
    # Get the range owned by this worker
    dim = dst_params["key"]
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
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
    nworkers = get_nworkers(comm)
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Vector{Int64} = []
    for partition_idx = 1:npartitions
        # `split_len` gives us the range that this partition needs
        partitionrange = split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap =
            max(startidx, partitionrange.start) <= min(endidx, partitionrange.stop)

        # If they do overlap, then serialize the overlapping slice
        ser(
            io,
            view(
                part,
                fill(:, dim - 1)...,
                if rangesoverlap
                    max(1, partitionrange.start - startidx + 1):min(
                        size(part, dim),
                        partitionrange.stop - startidx + 1,
                    )
                else
                    # Return zero length for this dimension
                    1:0
                end,
                fill(:, ndims(part) - dim)...,
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

function Rebalance(
    part::DataFrame,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
)
    # Get the range owned by this worker
    dim = 1
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    len = size(part, dim)
    scannedstartidx = MPI.Exscan(len, +, comm)
    startidx = worker_idx == 1 ? 1 : scannedstartidx + 1
    endidx = startidx + len - 1

    # Get functions for serializing/deserializing
    ser = Arrow.write
    # TODO: Use JLD for ser/de for arrays
    # TODO: Ensure that we are properly handling intermediate arrays or
    # dataframes that are empty (especially because they may not have their
    # ndims or dtype or schema). We probably are because dataframes that are
    # empty should concatenate properly. We just need to be sure to not expect
    # every partition to know what its schema is. We can however expect each
    # partition of an array to know its ndims.
    de = x -> DataFrames.DataFrame(Arrow.Table(IOBuffer(x)))

    # Construct buffer to send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Vector{Int64} = []
    for partition_idx = 1:npartitions
        # `split_len` gives us the range that this partition needs
        partitionrange = split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap =
            max(startidx, partitionrange.start) <= min(endidx, partitionrange.stop)

        # If they do overlap, then serialize the overlapping slice
        ser(
            io,
            view(
                part,
                fill(:, dim - 1)...,
                if rangesoverlap
                    max(1, partitionrange.start - startidx + 1):min(
                        size(part, dim),
                        partitionrange.stop - startidx + 1,
                    )
                else
                    # Return zero length for this dimension
                    1:0
                end,
                fill(:, ndims(part) - dim)...,
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

function Distribute(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T}
    # TODO: Determine whether copy is needed
    copy(SplitBlock(part, dst_params, 1, 1, comm, "Memory", Dict{String,Any}()))
end

# If this is a grouped data frame or nothing (the result of merging
# a grouped data frame is nothing), we consolidate by simply returning
# nothing.
Consolidate(part::Union{Nothing, GroupedDataFrame}, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing

function Consolidate(part::AbstractArray, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm)
    kind, sendbuf = tobuf(part)
    recvvbuf = buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized

    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    part = merge_on_executor(
        kind,
        recvvbuf,
        get_nworkers(comm);
        key = src_params["key"],
    )
    part
end

function Consolidate(part::DataFrame, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm)
    kind, sendbuf = tobuf(part)
    recvvbuf = buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized

    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    part = merge_on_executor(
        kind,
        recvvbuf,
        get_nworkers(comm);
        key = 1,
    )
    part
end

DistributeAndShuffle(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T} =
    SplitGroup(part, dst_params, 1, 1, comm, "Memory", Dict{String,Any}(), store_splitting_divisions = true)

function Shuffle(
    part::DataFrame,
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
    rev = dst_params["rev"]
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    divisions_by_worker = if haskey(dst_params, "divisions_by_worker")
        dst_params["divisions_by_worker"] # list of min-max tuples
    else 
        get_divisions(dst_params["divisions"], nworkers)
    end # list of min-max tuple lists
    if rev
        reverse!(divisions_by_worker)
    end

    # Perform shuffle
    partition_idx_getter(val) = get_partition_idx_from_divisions(
        val,
        divisions_by_worker,
        boundedlower = boundedlower,
        boundedupper = boundedupper,
    )
    res = begin
        gdf = if !isempty(part)
            # Compute the partition to send each row of the dataframe to
            DataFrames.transform!(part, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)

            # Group the dataframe's rows by what partition to send to
            gdf = DataFrames.groupby(part, :banyan_shuffling_key, sort = true)
            gdf
        else
            nothing
        end

        # Create buffer for sending dataframe's rows to all the partitions
        io = IOBuffer()
        nbyteswritten = 0
        df_counts::Vector{Int64} = []
        for partition_idx = 1:nworkers
            Arrow.write(
                io,
                if !isnothing(gdf) && (banyan_shuffling_key = partition_idx,) in keys(gdf)
                    gdf[(banyan_shuffling_key = partition_idx,)]
                else
                    empty(part)
                end,
            )
            push!(df_counts, io.size - nbyteswritten)
            nbyteswritten = io.size
        end
        sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), df_counts)

        # Create buffer for receiving pieces
        sizes = MPI.Alltoall(MPI.UBuffer(df_counts, 1), comm)
        recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

        # Perform the shuffle
        MPI.Alltoallv!(sendbuf, recvbuf, comm)

        # Return the concatenated dataframe
        things_to_concatenate = [
            DataFrames.DataFrame(
                Arrow.Table(IOBuffer(view(recvbuf.data, displ+1:displ+count))),
                copycols = false,
            ) for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
        ]
        res = length(things_to_concatenate) == 1 ? things_to_concatenate[1] : vcat(things_to_concatenate...)
        if :banyan_shuffling_key in propertynames(res)
            DataFrames.select!(res, Not(:banyan_shuffling_key))
        end

        res
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
        global splitting_divisions
        splitting_divisions[res] =
            (divisions_by_worker[worker_idx], !hasdivision || worker_idx != firstdivisionidx, !hasdivision || worker_idx != lastdivisionidx)
    end

    res
end

function Shuffle(
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
    rev = dst_params["rev"]
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    divisions_by_worker = if haskey(dst_params, "divisions_by_worker")
        dst_params["divisions_by_worker"] # list of min-max tuples
    else 
        get_divisions(dst_params["divisions"], nworkers)
    end # list of min-max tuple lists
    if rev
        reverse!(divisions_by_worker)
    end

    # Perform shuffle
    partition_idx_getter(val) = get_partition_idx_from_divisions(
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
        a_counts::Vector{Int64} = []
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
        global splitting_divisions
        splitting_divisions[res] =
            (divisions_by_worker[worker_idx], !hasdivision || worker_idx != firstdivisionidx, !hasdivision || worker_idx != lastdivisionidx)
    end

    res
end
