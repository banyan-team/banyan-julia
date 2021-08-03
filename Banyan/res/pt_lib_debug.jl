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
) = begin
    GC.gc()
    worker_idx = get_worker_idx(comm)
    # println("At start of returning null worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
    nothing
end

# TODO: Simplify the way we do locations. Locations should be in separate
# packages with separate location constructors and separate splitting/merging
# functions

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
    # We check if it's a file because for items on disk, files are HDF5
    # datasets while directories contain Parquet, CSV, or Arrow datasets
    path = getpath(loc_params["path"])
    # # # # println("In ReadBlock")
    # # @show path
    # # @show isfile(loc_params["path"])
    # # @show HDF5.ishdf5(loc_params["path"])
    if (loc_name == "Disk" && HDF5.ishdf5(loc_params["path"])) || (
        loc_name == "Remote" &&
        (occursin(".h5", loc_params["path"]) || occursin(".hdf5", loc_params["path"]))
    )
        f = h5open(path, "r")
        # @show keys(f)
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
        dset = dset[
            [
                if i == dim
                    split_len(size(dset, dim), batch_idx, nbatches, comm)
                else
                    Colon()
                end
                for i in 1:ndims(dset)
            ]...
        ]
        close(f)
        # end
        # # # # println("In ReadBlock")
        # # @show first(dset)
        return dset
    end

    # Handle single-file replicated objects
    if loc_name == "Disk" && isfile(path)
        # # # println("In Read")
        # @show path
        res = deserialize(path)
        # @show res
        return res
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
            # is maybe just an intermediate variable only used for this stage
            return nothing
        end
    end

    # Iterate through files and identify which ones correspond to the range of
    # rows for the batch currently being processed by this worker
    nrows = loc_params["nrows"]
    rowrange = split_len(nrows, batch_idx, nbatches, comm)
    dfs::Vector{DataFrame} = []
    rowsscanned = 0
    for file in loc_params["files"]
        newrowsscanned = rowsscanned + file["nrows"]
        filerowrange = (rowsscanned+1):newrowsscanned
        # Check if te file corresponds to the range of rows for the batch
        # currently being processed by this worker
        if isoverlapping(filerowrange, rowrange)
            # Deterine path to read from
            path = getpath(file["path"])

            # Read from location depending on data format
            readrange =
                max(rowrange.start, filerowrange.start):min(
                    rowrange.stop,
                    filerowrange.stop,
                )
            header = 1
            if endswith(path, ".csv")
                f = CSV.File(
                    path,
                    header = header,
                    skipto = header + readrange.start - filerowrange.start + 1,
                    footerskip = filerowrange.stop - readrange.stop,
                )
                push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
            elseif endswith(path, ".parquet")
                f = Parquet.File(
                    path,
                    rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
                )
                push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
            elseif endswith(path, ".arrow")
                rbrowrange = filerowrange.start:(filerowrange.start-1)
                for tbl in Arrow.Stream(path)
                    rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+length(tbl))
                    if isoverlapping(rbrowrange, rowrange)
                        readrange =
                            max(rowrange.start, rbrowrange.start):min(
                                rowrange.stop,
                                rbrowrange.stop,
                            )
                        df = DataFrame(tbl)
                        df = df[
                            (readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1),
                            :,
                        ]
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
    # NOTE: If this partition is empty, it is possible that the result is
    # schemaless (unlike the case with HDF5 where the resulting array is
    # guaranteed to have its ndims correct) and so if a split/merge/cast
    # function requires the schema (for example for grouping) then it must be
    # sure to take that account
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
    rev = params["rev"] # Passed in ReadBlock
    nworkers = get_nworkers(comm)
    npartitions = nworkers * nbatches
    partition_divisions = get_divisions(divisions, npartitions)

    # Get the divisions that are relevant to this batch by iterating
    # through the divisions in a stride and consolidating the list of divisions
    # for each partition. Then, ensure we use boundedlower=true only for the
    # first batch and boundedupper=true for the last batch.
    curr_partition_divisions = []
    for worker_division_idx = 1:nworkers
        for batch_division_idx = 1:nbatches
            partition_division_idx =
                (worker_division_idx - 1) * nbatches + batch_division_idx
            if batch_division_idx == batch_idx
                p_divisions = partition_divisions[partition_division_idx]
                push!(
                    curr_partition_divisions,
                    (first(p_divisions)[1], last(p_divisions)[2]),
                )
            end
        end
    end

    # Read in each batch and shuffle it to get the data for this partition
    parts = []
    for i = 1:nbatches
        # Read in data for this batch
        part = ReadBlock(src, params, i, nbatches, comm, loc_name, loc_params)

        # Shuffle the batch and add it to the set of data for this partition
        push!(
            parts,
            Shuffle(
                part,
                Dict(),
                params,
                comm,
                boundedlower = i == 1,
                boundedupper = i == nbatches,
            ),
        )
    end

    # Concatenate together the data for this partition
    # res = merge_on_executor(parts, dims=isa_array(first(parts)) ? key : 1)
    # # @show parts
    res = merge_on_executor(parts...; key = key)
    # # @show res

    # Store divisions
    global splitting_divisions
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    splitting_divisions[res] =
        (partition_divisions[partition_idx], partition_idx > 1, partition_idx < npartitions)

    @show typeof(res)
    res
end

function format_bytes(bytes, decimals = 2)
    bytes == 0 && return "0 Bytes"
    k = 1024
    dm = decimals < 0 ? 0 : decimals
    sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    i = convert(Int, floor(log(bytes) / log(k)))
    return string(round((bytes / ^(k, i)), digits=dm)) * " " * sizes[i+1];
end

format_available_memory() = format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

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
    # if batch_idx > 1
        GC.gc()
    # end

    # Get path of directory to write to
    path = loc_params["path"]
    if startswith(path, "http://") || startswith(path, "https://")
        error("Writing to http(s):// is not supported")
    elseif startswith(path, "s3://")
        path = getpath(path)
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at mnt/<bucket name>
    end

    # # # println("In Write where batch_idx=$batch_idx")

    # Write file for this partition
    worker_idx = get_worker_idx(comm)
    # println("Writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
    idx = get_partition_idx(batch_idx, nbatches, comm)
    if isa_df(part)
        # Create directory if it doesn't exist
        # TODO: Avoid this and other filesystem operations that would be costly
        # since S3FS is being used
        if !isdir(path)
            mkpath(path)
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

        nrows = size(part, 1)
        if endswith(path, ".parquet")
            partfilepath = joinpath(path, "part$idx" * "_nrows=$nrows.parquet")
            Parquet.write_parquet(partfilepath, part)
        elseif endswith(path, ".csv")
            partfilepath = joinpath(path, "part$idx" * "_nrows=$nrows.csv")
            CSV.write(partfilepath, part)
        else
            partfilepath = joinpath(path, "part$idx" * "_nrows=$nrows.arrow")
            Arrow.write(partfilepath, part)
        end
        src
        MPI.Barrier(comm)
        # TODO: Delete all other part* files for this value if others exist
    elseif isa_array(part)
        # if loc_name == "Disk"
        #     # Technically we don't have to do this since when we read we can
        #     # check if location is Disk
        #     path *= ".hdf5"
        # end
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

        # whole_size =
        #     nbatches == 1 ?
        #     MPI.Reduce(size(part), (a, b) -> indexapply(+, a, b, index = dim), 0, comm) :
        #     nothing

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
        dataset_writing_permission = "cw"
        force_overwrite = true

        info = MPI.Info()

        # Write out to an HDF5 dataset differently depending on whether there
        # are multiple batches per worker or just one per worker
        if nbatches == 1
            # # # println("In Write where nbatches==1")
            # Determine the offset into the resulting HDF5 dataset where this
            # worker should write
            offset = MPI.Exscan(size(part, dim), +, comm)
            if worker_idx == 1
                offset = 0
            end
            # @show offset

            # Create file if not yet created
            # @show path
            # TODO: Figure out why sometimes a deleted file still `isfile`
            # @show isfile(path) # This is true while below is false
            # @show HDF5.ishdf5(path)
            # NOTE: We used INDEPENDENT here on the most recent run
            # f = h5open(path, "cw", fapl_mpio=(comm, info), dxpl_mpio=HDF5.H5FD_MPIO_INDEPENDENT)
            f = h5open(path, "cw", fapl_mpio=(comm, info), dxpl_mpio=HDF5.H5FD_MPIO_COLLECTIVE)
            close(f)
            
            # NOTE: The below is an alternative
            # MPI.Barrier(comm)
            # if worker_idx == 1
            #     # # # println("Before h5open")
            #     # f = h5open(path, "cw", comm, info)
            #     # f = h5open(path, "cw", fapl_mpio=(comm, info), dxpl_mpio=HDF5.H5FD_MPIO_INDEPENDENT)
            #     f = h5open(path, "cw")
            #     # # # println("Before close")
            #     close(f)
            #     # # # println("After close")    
            # end

            MPI.Barrier(comm)

            
            # pathexists = HDF5.ishdf5(path)
            # pathexists = MPI.bcast(pathexists, 0, comm)
            # if !pathexists
            #     # # # println("Before h5open")
            #     f = h5open(path, "w", comm, info)
            #     # # # println("Before close")
            #     close(f)
            #     # # # println("After close")
            # end
            # MPI.Barrier(comm)

            # Open file for writing data
            f = h5open(path, "r+", comm, info)

            # Overwrite existing dataset if found
            # TODO: Return error on client side if we don't want to allow this
            if force_overwrite && haskey(f, group)
                delete_object(f[group])
            end

            # Create dataset
            whole_size = indexapply(+, size(part), offset, index=dim)
            whole_size = MPI.bcast(whole_size, nworkers-1, comm) # Broadcast dataset size to all workers
            # @show eltype(part)
            # @show whole_size
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
                    end
                    for d = 1:ndims(dset)
                ]...,
            )

            # Close file
            # # # println("Before close dset on $worker_idx")
            close(dset)
            # # # println("Before close f on $worker_idx")
            close(f)
            # # # println("Before barrier on $worker_idx")
            MPI.Barrier(comm)
            # # # println("After barrier on $worker_idx")

            # NOTE: If we are removing MPI barriers it might be a good idea to
            # keep the last barrier because of an issue mentioned here:
            # https://support.hdfgroup.org/HDF5/release/known_problems/previssues.html
            # > the application should close the file, issue an MPI_Barrier(comm),
            # > then reopen the file before and after each collective write. 

            # # Make the last worker create the dataset (since it can compute
            # # the total dataset size using its offset)
            # if worker_idx == nworkers
            #     # # # println("Entering if")
            #     # @show group
            #     # @show nbatches
            #     whole_size = indexapply(+, size(part), offset, index=dim)
            #     if force_overwrite && isfile(path)
            #         h5open(path, "r+") do f
            #             if haskey(f, group_prefix)
            #                 delete_object(f[group_prefix])
            #             end
            #         end
            #     end
            #     h5open(path, dataset_writing_permission) do fid
            #         # If there are multiple batches, each batch just gets written
            #         # to its own group
            #         # TODO: Use `dataspace` instead of similar since the array
            #         # may not fit in memory
            #         fid[group] = similar(part, whole_size)
            #         # @show keys(fid)
            #         # @show fid
            #         close(fid[group])
            #     end
            #     # # # println("Exiting if")
            # end
            # # touch(path * "_is_ready")

            # # Wait until all workers have the file
            # # TODO: Maybe use a broadcast so that each node is only blocked on
            # # the main node
            # MPI.Barrier(comm)

            # # Open the file for writing
            # h5open(path, "r+") do f
            #     # Get the group to write to
            #     dset = f[group]

            #     # Mutate only the relevant subsection
            #     # TODO: Use Exscan here and below to determine range to write to
            #     # scannedstartidx = MPI.Exscan(len, +, comm)
            #     setindex!(
            #         dset,
            #         part,
            #         [
            #             # d == dim ? split_len(whole_size[dim], batch_idx, nbatches, comm) :
            #             if d == dim
            #                 (offset+1):(offset+size(part, dim))
            #             else 
            #                 Colon()
            #             end
            #             for d = 1:ndims(dset)
            #         ]...,
            #     )

            #     close(dset)
            # end
        else
            # TODO: See if we have missing `close`s or missing `fsync`s or extra `MPI.Barrier`s
            fsync_file(p) = open(p) do f
                # TODO: Maybe use MPI I/O method for fsync instead
                ccall(:fsync, Cint, (Cint,), fd(f))
            end

            # Create dataset on head node
            # if partition_idx == 1
            #     # # Force delete any existing dataset if needed
            #     # # NOTE: We use the same reasoning by Rick Zamora here -
            #     # # https://github.com/dask/dask/issues/7466. We expect users
            #     # # who want to read from and write to the same file to do so
            #     # # correctly. Because technically... this could delete a group
            #     # # that we are still reading from. But we expect users to be
            #     # # careful about this and create a temporary new file if
            #     # # necessary or to keep the whole thing in memory if needed.
            #     # if force_overwrite && isfile(path)
            #     #     h5open(path, "r+") do f
            #     #         # # # println("Deleting")
            #     #         # @show group_prefix
            #     #         # @show keys(f)
            #     #         delete_object(f[group_prefix])
            #     #         # @show keys(f)
            #     #     end
            #     # end

            #     # Create the file
            #     h5open(path, dataset_writing_permission) do fid end
            # end
            # # # # println("Entering if")
            # # @show group
            # # @show nbatches
            # if partition_idx == 1
            #     if force_overwrite && isfile(path)
            #         h5open(path, "r+") do f
            #             delete_object(f[group])
            #         end
            #     end
            #     h5open(path, dataset_writing_permission) do fid
            #         create_dataset(fid, group, part)
            #     end
            # else
            #     # Wait for file to be created
            #     if batch_idx == 1
            #         # NOTE: Every worker must have at least 1 batch
            #         MPI.Barrier(comm)
            #     end

            #     # Write this batch's partition to a separate dataset
            #     h5open(path, "r+") do fid
            #         # If there are multiple batches, each batch just gets written
            #         # to its own group
            #         create_dataset(fid, group, part)
            #         # @show keys(fid)
            #         # @show fid
            #     end
            # end
            # # # # println("Exiting if")

            # # Wait for the file to be created
            # # NOTE: It is not sufficient to gather below because some nodes
            # # will send the requested length to the head node and then
            # # immediately proceed to trying to create 
            # MPI.Barrier(comm)

            # Create the file if not yet created
            if batch_idx == 1
                # f = h5open(path, "cw", comm, info) do _ end
                # f = h5open(path, "cw", comm, info)
                # @show isfile(path)
                # @show HDF5.ishdf5(path)
                f = h5open(path, "cw", fapl_mpio=(comm, info), dxpl_mpio=HDF5.H5FD_MPIO_COLLECTIVE)
                close(f)

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after creating file with available memory: $(format_available_memory())")

                # # Substitute for `h5open` with "cw" which doesn't seem to work
                # pathexists = HDF5.ishdf5(path)
                # # pathexists = MPI.bcast(pathexists, 0, comm)
                # MPI.Barrier(comm)
                # if !pathexists
                #     # # # # println("Before h5open")
                #     # f = h5open(path, "w", comm, info)
                #     # # # # println("Before close")
                #     # close(f)
                #     # # # # println("After close")
                #     if worker_idx == 1
                #         f = h5open(path, "w")
                #         close(f)
                #     end
                #     # MPI.bcast(-1, 0, comm)
                # end
                # MPI.Barrier(comm)
                # # h5open(path * "_intermediates", "w", comm, info) do _ end

                MPI.Barrier(comm)
            end

            # TODO: Maybe use collective I/O with
            # `dxpl_mpio=HDF5.H5FD_MPIO_COLLECTIVE` for higher performance
            # TODO: Maybe use fsync to flush out after the file is closed
            # TODO: Use a separate file for intermediate datasets so that we
            # don't create a bunch of extra datasets everytime we try to write

            # h5open(path, "r+", comm, info) do f
            f = h5open(path, "r+", comm, info)
            # Allocate all datasets needed by gathering all sizes to the head
            # node and making calls from there
            part_lengths = MPI.Allgather(size(part, dim), comm)

            # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after Allgather with available memory: $(format_available_memory())")
            # @show part_lengths
            partdsets = [
                begin
                    idx = get_partition_idx(batch_idx, nbatches, worker_i)
                    group = group_prefix*"_part$idx"*"_dim=$dim"
                    # @show keys(f)
                    # If there are multiple batches, each batch just gets written
                    # to its own group
                    # @show indexapply(_ -> part_length, size(part), index=dim)
                    dataspace_size = indexapply(_ -> part_length, size(part), index=dim)
                    new_dset = create_dataset(f, group, eltype(part), (dataspace_size, dataspace_size))
                    # @show eltype(part)
                    # # @show keys(fid)
                    # # @show fid
                    # close(fid[group])
                    new_dset
                end
                for (worker_i, part_length) in enumerate(part_lengths)
            ]   

            # Wait for the head node to allocate all the datasets for this
            # batch index
            # TODO: Ensure that nothing doesn't cause bcast to just become a
            # no-op or something like that
            # MPI.bcast(nothing, 0, comm)
            # TODO: Try removing this barrier
            MPI.Barrier(comm)

            # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after creating datasets with available memory: $(format_available_memory())")

            # Each worker then writes their partition to a separate dataset
            # in parallel
            # group = group_prefix*"_part$partition_idx"*"_dim=$dim"
            # f[group][fill(Colon(), ndims(part))...] = part
            partdsets[worker_idx][fill(Colon(), ndims(part))...] = part
            # close(fid[group])

            # Close (flush) all the intermediate datasets that we have created
            # TODO: Try removing this barrier
            MPI.Barrier(comm)
            for partdset in partdsets
                close(partdset)
            end
            # TODO: Try removing this barrier
            MPI.Barrier(comm)

            # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after writing to each dataset with available memory: $(format_available_memory())")

            # # If we aren't yet on the last batch, then we are only
            # # creating and writing to intermediate datasets (a dataset
            # # for each partition). So we need to close these intermediate
            # # datasets. If we _are_ on the last batch, we will be simply
            # # reading from these intermediate batches and writing to the
            # # actual dataset we want to write to. So on the last batch (not
            # # covered by this if statement but handled later on) to delete
            # # the interemdiate datasets and close the final dataset that
            # # we wrote to.
            # if batch_idx < nbatches
            #     MPI.Barrier(comm)
            #     # for worker_i in 1:nworkers
            #     #     idx = get_partition_idx(batch_idx, nbatches, worker_i)
            #     #     group = group_prefix*"_part$idx"*"_dim=$dim"
            #     #     close(f[group])
            #     # end
            #     for partdset in partdsets
            #         close(partdset)
            #     end
            # end

            # Collect datasets from each batch and write into the final result dataset
            if batch_idx == nbatches
                # Get all intermediate datasets that have been written to by this worker
                partdsets = [
                    begin
                        # Determine what index partition this batch is
                        idx = get_partition_idx(batch_idx, nbatches, comm)

                        # Get the dataset
                        group = group_prefix*"_part$idx"*"_dim=$dim"  
                        f[group]
                    end
                    for batch_idx = 1:nbatches
                ]

                # Compute the size of all the batches on this worker
                # concatenated
                # whole_batch_length = 0
                # for batch_idx = 1:nbatches
                #     # # Determine what index partition this batch is
                #     # idx = get_partition_idx(batch_idx, nbatches, comm)

                #     # # Add up the whole batch size
                #     # group = group_prefix*"_part$idx"*"_dim=$dim"
                #     # whole_batch_length += size(f[group], dim)
                #     whole_batch_length += size(partdsets[batch_idx], dim)

                #     # for group_name in keys(f)
                #     #     if startswith(group_name, group_prefix)
                #     #         part_idx, dim =
                #     #             parse.(Int64, split(last(split(group_name, "_part")), "_dim="))
                #     #         whole_batch_size =
                #     #             isnothing(whole_batch_size) ? size(f[group_name]) :
                #     #             indexapply(
                #     #                 +,
                #     #                 whole_batch_size,
                #     #                 size(f[group_name]),
                #     #                 index = dim,
                #     #             )
                #     #     end
                #     # end
                # end
                whole_batch_length = sum([size(partdset, dim) for partdset in partdsets])

                # Determine the offset into the resulting HDF5 dataset where this
                # worker should write
                offset = MPI.Exscan(whole_batch_length, +, comm)
                if worker_idx == 1
                    offset = 0
                end

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after Exscan with available memory: $(format_available_memory())")
    
                # Make the last worker create the dataset (since it can compute
                # the total dataset size using its offset)
                # NOTE: It's important that we use the last node since the
                # last node has the scan result
                # # # println("Entering if")
                # @show group
                # @show nbatches
                whole_size = indexapply(_ -> offset + whole_batch_length, size(part), index=dim)
                whole_size = MPI.bcast(whole_size, nworkers-1, comm) # Broadcast dataset size to all workers
                # @show whole_size
                # The permission used here is "r+" because we already
                # created the file on the head node
                # Delete the dataset if needed before we write the
                # dataset to which each batch will write its chunk
                if force_overwrite && haskey(f, group_prefix)
                    delete_object(f[group_prefix])
                end

                # If there are multiple batches, each batch just gets written
                # to its own group
                # # # println("Creating")
                # @show group_prefix
                # @show keys(f)
                dset = create_dataset(f, group_prefix, eltype(part), (whole_size, whole_size))
                # @show keys(f)
                # # @show keys(fid)
                # # @show fid
                # close(fid[group_prefix])
                # # # println("Exiting if")
    
                # Wait until all workers have the file
                # TODO: Maybe use a broadcast so that each node is only blocked on
                # the last node which is where the file is creating
                # TODO: Try removing this barrier
                MPI.Barrier(comm)

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after creating final dataset with available memory: $(format_available_memory())")
    
                # Write out each batch
                # @show HDF5.ishdf5(path)
                # @show keys(f)
                # dset = f[group_prefix]
                batchoffset = offset
                for batch_i = 1:nbatches
                    partdset = partdsets[batch_i]

                    # Determine what index partition this batch is
                    idx = get_partition_idx(batch_i, nbatches, comm)

                    # Write
                    group = group_prefix*"_part$idx"*"_dim=$dim"
                    # # @show (batchoffset+1):batchoffset+size(f[group], dim)
                    # # @show size(dset)
                    # # @show size(f[group][fill(Colon(), ndims(dset))...])
                    # # @show eltype(dset)
                    # # @show eltype(f[group])
                    # # @show HDF5.ishdf5(path)
                    # # @show keys(f)
                    # # @show sum(f[group][fill(Colon(), ndims(dset))...])
                    # @show (batchoffset+1):batchoffset+size(partdset, dim)
                    # @show size(dset)
                    # @show size(partdset[fill(Colon(), ndims(dset))...])
                    # @show eltype(dset)
                    # @show eltype(partdset)
                    # @show HDF5.ishdf5(path)
                    # @show keys(f)
                    # @show sum(partdset[fill(Colon(), ndims(dset))...])
                    # TOODO: Maynee remoce printlns to make it work
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
                    # @show sum(getindex(dset, [
                    #     if d == dim
                    #         (batchoffset+1):batchoffset+size(partdset, dim)
                    #     else
                    #         Colon()
                    #     end
                    #     # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
                    #     for d = 1:ndims(dset)
                    # ]...))

                    # # @show size(dset)

                    # Update the offset of this batch
                    batchoffset += size(partdset, dim)
                    close(partdset)
                    partdset = nothing

                    # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after writing batch $batch_i with available memory: $(format_available_memory())")
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

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after writing each part with available memory: $(format_available_memory())")

                # Then, delete all data for all groups on the head node
                # # @show sum(dset[fill(Colon(), ndims(dset))...])
                for worker_i = 1:nworkers
                    for batch_i = 1:nbatches
                        idx = get_partition_idx(batch_i, nbatches, worker_i)
                        group = group_prefix*"_part$idx"*"_dim=$dim"
                        # # # println("Deleting a group")
                        # @show group
                        # @show keys(f)
                        delete_object(f[group])
                        # @show keys(f)
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
                # # @show worker_idx
                # MPI.Barrier(comm)
                # # @show worker_idx
            # end
                # @show worker_idx
                MPI.Barrier(comm)
                # @show worker_idx 

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after deleting all objects with available memory: $(format_available_memory())")
            end
            close(f)
            f = nothing
            # TODO: Ensure that we are closing stuff everywhere before trying
            # to write
            # @show worker_idx
            MPI.Barrier(comm)
            # @show worker_idx 

            # # Allocate all datasets needed by gathering all sizes to the head
            # # node and making calls from there
            # part_lengths = MPI.Gather(size(part, dim), 0, comm)
            # if worker_idx == 1
            #     h5open(path, "r+") do fid
            #         for (worker_i, part_length) in enumerate(part_lengths)
            #             idx = get_partition_idx(batch_idx, nbatches, worker_i)
            #             group = group_prefix*"_part$idx"*"_dim=$dim"
            #             # @show keys(fid)
            #             # If there are multiple batches, each batch just gets written
            #             # to its own group
            #             # TODO: Use `dataspace` instead of similar since the array
            #             # may not fit in memory'
            #             # @show indexapply(_ -> part_length, size(part), index=dim)
            #             fid[group] = similar(part, indexapply(_ -> part_length, size(part), index=dim))
            #             # @show eltype(part)
            #             # # @show keys(fid)
            #             # # @show fid
            #             close(fid[group])
            #         end
            #     end
            #     fsync_file()
            # end

            # # Wait for the head node to allocate all the datasets for this
            # # batch index
            # # TODO: Ensure that nothing doesn't cause bcast to just become a
            # # no-op or something like that
            # # MPI.bcast(nothing, 0, comm)
            # MPI.Barrier(comm)

            # # Each worker then writes their partition to a separate dataset
            # # in parallel
            # group = group_prefix*"_part$partition_idx"*"_dim=$dim"
            # h5open(path, "r+") do fid
            #     # # @show keys(fid)
            #     fid[group][fill(Colon(), ndims(part))...] = part
            #     close(fid[group])
            # end
            # fsync_file()

            # # TODO: Maybe do a proper barrier or something other than exscan to ensure each dataset gets written out

            # # Collect datasets from each batch and write into the final result dataset
            # if batch_idx == nbatches
            #     # Compute the size of all the batches on this worker
            #     # concatenated
            #     whole_batch_length = 0
            #     h5open(path, "r") do f
            #         for batch_idx = 1:nbatches
            #             # Determine what index partition this batch is
            #             idx = get_partition_idx(batch_idx, nbatches, comm)

            #             # Add up the whole batch size
            #             group = group_prefix*"_part$idx"*"_dim=$dim"
            #             whole_batch_length += size(f[group], dim)

            #             # for group_name in keys(f)
            #             #     if startswith(group_name, group_prefix)
            #             #         part_idx, dim =
            #             #             parse.(Int64, split(last(split(group_name, "_part")), "_dim="))
            #             #         whole_batch_size =
            #             #             isnothing(whole_batch_size) ? size(f[group_name]) :
            #             #             indexapply(
            #             #                 +,
            #             #                 whole_batch_size,
            #             #                 size(f[group_name]),
            #             #                 index = dim,
            #             #             )
            #             #     end
            #             # end
            #         end
            #     end

            #     # Determine the offset into the resulting HDF5 dataset where this
            #     # worker should write
            #     offset = MPI.Exscan(whole_batch_length, +, comm)
            #     if worker_idx == 1
            #         offset = 0
            #     end
    
            #     # Make the last worker create the dataset (since it can compute
            #     # the total dataset size using its offset)
            #     if worker_idx == nworkers
            #         # NOTE: It's important that we use the last node since the
            #         # last node has the scan result
            #         # # # println("Entering if")
            #         # @show group
            #         # @show nbatches
            #         whole_size = indexapply(_ -> offset + whole_batch_length, size(part), index=dim)
            #         # @show whole_size
            #         # The permission used here is "r+" because we already
            #         # created the file on the head node
            #         h5open(path, "r+") do fid
            #             # Delete the dataset if needed before we write the
            #             # dataset to which each batch will write its chunk
            #             if force_overwrite && haskey(fid, group_prefix)
            #                 delete_object(fid[group_prefix])
            #             end

            #             # If there are multiple batches, each batch just gets written
            #             # to its own group
            #             # # # println("Creating")
            #             # @show group_prefix
            #             # @show keys(fid)
            #             fid[group_prefix] = similar(part, whole_size)
            #             # @show keys(fid)
            #             # # @show keys(fid)
            #             # # @show fid
            #             close(fid[group_prefix])
            #         end
            #         # # # println("Exiting if")
            #     end
    
            #     # Wait until all workers have the file
            #     # TODO: Maybe use a broadcast so that each node is only blocked on
            #     # the last node which is where the file is creating
            #     MPI.Barrier(comm)
    
            #     # Write out each batch
            #     # @show HDF5.ishdf5(path)
            #     h5open(path, "r+") do f
            #         # @show keys(f)
            #         dset = f[group_prefix]
            #         batchoffset = offset
            #         for batch_i = 1:nbatches
            #             # Determine what index partition this batch is
            #             idx = get_partition_idx(batch_i, nbatches, comm)
    
            #             # Write
            #             group = group_prefix*"_part$idx"*"_dim=$dim"
            #             # @show (batchoffset+1):batchoffset+size(f[group], dim)
            #             # @show size(dset)
            #             # @show size(f[group][fill(Colon(), ndims(dset))...])
            #             # @show eltype(dset)
            #             # @show eltype(f[group])
            #             # @show HDF5.ishdf5(path)
            #             # @show keys(f)
            #             # @show sum(f[group][fill(Colon(), ndims(dset))...])
            #             # TOODO: Maynee remoce printlns to make it work
            #             setindex!(
            #                 # We are writing to the whole dataset that was just
            #                 # created
            #                 dset,
            #                 # We are copying from the written HDF5 dataset for a
            #                 # particular batch
            #                 f[group][fill(Colon(), ndims(dset))...],
            #                 # We write to the appropriate split of the whole
            #                 # dataset
            #                 [
            #                     if d == dim
            #                         (batchoffset+1):batchoffset+size(f[group], dim)
            #                     else
            #                         Colon()
            #                     end
            #                     # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
            #                     for d = 1:ndims(dset)
            #                 ]...,
            #             )
            #             # @show sum(getindex(dset, [
            #                 if d == dim
            #                     (batchoffset+1):batchoffset+size(f[group], dim)
            #                 else
            #                     Colon()
            #                 end
            #                 # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
            #                 for d = 1:ndims(dset)
            #             ]...))

            #             # # @show size(dset)

            #             # Update the offset of this batch
            #             batchoffset += size(f[group], dim)
            #             close(f[group])
            #         end
            #         close(dset)
            #     end
            #     fsync_file()

            #     # Wait until all the data is written
            #     MPI.Barrier(comm)
            #     # NOTE: Issue is that the barrier here doesn't ensure that all
            #     # processes have written in the previous step
            #     # TODO: Use a broadcast here

            #     # Then, delete all data for all groups on the head node
            #     if worker_idx == 1
            #         h5open(path, "r+") do f
            #             # @show sum(f[group_prefix][fill(Colon(), ndims(f[group_prefix]))...])
            #             for worker_i = 1:nworkers
            #                 for batch_i = 1:nbatches
            #                     idx = get_partition_idx(batch_i, nbatches, worker_i)
            #                     group = group_prefix*"_part$idx"*"_dim=$dim"
            #                     # # # println("Deleting a group")
            #                     # @show group
            #                     # @show keys(f)
            #                     delete_object(f[group])
            #                     # @show keys(f)
            #                 end
            #             end
            #             # TODO: Ensure that closing (flushing) HDF5 datasets
            #             # and files is sufficient. We might additionally have
            #             # to sync to ensure that the content is actually
            #             # written to disk or to S3
            #             # TODO: Use create_dataset passing in dtype and dimensions
            #         end
            #         fsync_file()
            #     end

            #     # TODO: Determine whether this is necessary. This barrier might
            #     # be necessary to ensure that all groups are deleted before we
            #     # continue.
            #     MPI.Barrier(comm)
            # end
        end

        # @show path
        # @show HDF5.ishdf5(path)

        # Wait till the dataset is created
        # # # # println("Starting to wait")

        # Write part to the dataset
        # # @show f
        # # @show keys(f)
        # # # # println("Opened")
        # TODO: Use `view` instead of `getindex` in the call to
        # `split_on_executor` here if HDF5 doesn't support this kind of usage
        # TODO: Determine why `readmmap` gave an "Error getting offset"
        # TODO: Don't merge back into an mmapped dataset if not needed
        # if HDF5.ismmappable(dset)
        #     dset = HDF5.readmmap(dset)
        #     close(f)
        #     dsubset = split_on_executor(dset, dim, batch_idx, nbatches, comm)
        #     dsubset .= part
        # else
        # dset = read(dset)
        # # @show size(dset, dim)
        # # @show batch_idx
        # # @show nbatches
        # # @show whole_size
        # # @show split_len(whole_size[dim], batch_idx, nbatches, comm)
        # # @show size(dset)
        # # @show size(part)
        
        # dsubset = split_on_executor(dset, dim, batch_idx, nbatches, comm)
        # NOTE: For writing to HDF5 datasets we can't just use
        # split_on_executor because we aren't reading a copy; instead, we are
        # writing to a slice
        # # @show first(part)
        # # @show first(dset[:])
        # dsubset .= part
        # close(f)
        # end
        # # # # println("Done writing")

        # TODO: Make this work for variable-sized element type
        # TODO: Support arrays and reductions with variable-sized elements
        # TODO: Maybe use Arrow
    elseif get_partition_idx(batch_idx, nbatches, comm) == 1
        # This must be on disk; we don't support Julia serialized objects
        # as a remote location yet. We will need to first refactor locations
        # before we add support for that.
        if isa_gdf(part)
            part = nothing
        end
        # # # println("In Write")
        # @show part
        # @show path
        serialize(path, part)
    end
    # # println("End of writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches")
end

global partial_merges = Set()

function SplitBlock(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    global partial_merges
    if isnothing(src) || objectid(src) in partial_merges
        src
    else
        split_on_executor(
            src,
            isa_array(src) ? params["key"] : 1,
            batch_idx,
            nbatches,
            comm,
        )
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
    global partial_merges
    if isnothing(src) || objectid(src) in partial_merges
        # src is [] if we are partially merged (because as we iterate over
        # batches we take turns between splitting and merging)
        return nothing
    end

    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    npartitions = get_npartitions(nbatches, comm)

    # Ensure that this partition has a schema that is suitable for usage
    # here. We have to do this for `Shuffle` and `SplitGroup` (which is
    # used by `DistributeAndShuffle`)
    # if isa_df(src) && isempty(src)
    if isempty(src)
        # inv: !(key in names(df))
        # src[!, key] = []
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
    # # @show key
    res = if isa_df(src)
        # TODO: Do the groupby and filter on batch_idx == 1 and then share
        # among other batches
        filter(row -> partition_idx_getter(row[key]) == partition_idx, src)
    elseif isa_array(src)
        cat(
            filter(
                e -> partition_idx_getter(e) == partition_idx,
                eachslice(src, dims = key),
            )...;
            dims = key,
        )
    else
        throw(ArgumentError("Expected array or dataframe to distribute and shuffle"))
    end
    worker_idx = get_worker_idx(comm)
    if is_debug_on()
        # @show batch_idx worker_idx src res
    end

    # Store divisions
    global splitting_divisions
    splitting_divisions[res] = (
        divisions_by_partition[partition_idx],
        boundedlower || partition_idx > 1,
        boundedupper || partition_idx < npartitions,
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
    global partial_merges

    if batch_idx == 1 || batch_idx == nbatches
        GC.gc()
    end

    # # # println("In Merge where batch_idx==$batch_idx")

    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
    worker_idx = get_worker_idx(comm)
    # println("Merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
    if isnothing(src) || objectid(src) in partial_merges
        # We only need to concatenate partitions if the source is nothing.
        # Because if the source is something, then part must be a view into it
        # and no data movement is needed.

        # dim = isa_array(part) ? params["key"] : 1
        key = params["key"]

        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        npartitions = get_npartitions(nbatches, comm)

        # Concatenate across batches
        if batch_idx == 1
            src = []
            push!(partial_merges, objectid(src))
        end
        push!(src, part)
        if batch_idx == 1 || batch_idx == nbatches
            # println("At start of merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory()) and used: $(format_bytes(Base.summarysize(src)))")
        end
        if batch_idx == nbatches
            # println("At start of merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
            delete!(partial_merges, objectid(src))

            # TODO: Test that this merges correctly
            # src = merge_on_executor(src...; dims=dim)
            src = merge_on_executor(src...; key = key)
            # println("After locally merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")

            # Concatenate across workers
            nworkers = get_nworkers(comm)
            if nworkers > 1
                src = Consolidate(src, params, Dict(), comm)
                # println("After distributing and merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
            end
            if is_debug_on()
                # @show partial_merges
            end
            # delete!(partial_merges, src)
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
    # @show loc_name
    if loc_name == "Value"
        loc_params["value"]
    elseif loc_name == "Disk"
        # p = joinpath(loc_params["path"] * "_part")
        # if isfile(p)
        #     # Check if there is a single partition spilled to disk,
        #     # indicating that we should then simply deserialize and return
        #     open(p) do f
        #         deserialize(f)
        #     end
        # elseif isdir(loc_params["path"])
        #     ReadBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        # else
        #     nothing
        # end
        params["key"] = 1
        res = ReadBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        # # # println("In CopyFrom")
        # # @show length(res)
        # @show res
        res
    elseif loc_name == "Remote"
        params["key"] = 1
        ReadBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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
    # # # println("In CopyTo")
    # @show get_partition_idx(batch_idx, nbatches, comm)
    # @show get_npartitions(nbatches, comm)
    # @show get_worker_idx(comm)
    # @show MPI.Comm_rank(MPI.COMM_WORLD)
    # @show part
    # @show loc_name
    if loc_name == "Memory"
        src = part
    else
        # If we are copying to an external location we only want to do it on
        # the first worker since assuming that `on` is either `everywhere` or
        # `head`, so any batch on the first worker is guaranteed to have the
        # value that needs to be copied (either spilled to disk if None or
        # sent to remote storage).
        if loc_name == "Disk"
            # # TODO: Add case for an array by writing to HDF5 dataset
            # if isa_df(part) || isa_array(part)
            #     Write(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
            # else
            #     p = joinpath(loc_params["path"] * "_part")
            #     open(p, "w") do f
            #         serialize(f, part)
            #     end
            # end
            # TODO: Don't rely on ReadBlock, Write in CopyFrom, CopyTo and
            # instead do something more elegant
            if get_partition_idx(batch_idx, nbatches, comm) == 1
                params["key"] = 1
                Write(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
            end
            # TODO: Remove this barrier if not needed to ensure correctness
            MPI.Barrier(comm)
        elseif loc_name == "Remote"
            if get_partition_idx(batch_idx, nbatches, comm) == 1
                params["key"] = 1
                Write(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
            end
            # TODO: Remove this barrier if not needed to ensure correctness
            MPI.Barrier(comm)
        elseif loc_name == "Client"
            # TODO: Ensure this only sends once
            # # # println("Sending to client")
            # @show part
            if get_partition_idx(batch_idx, nbatches, comm) == 1
                send_to_client(loc_params["value_id"], part)
            end
            # TODO: Remove this barrier if not needed to ensure correctness
            MPI.Barrier(comm)
        else
            error("Unexpected location")
        end
    end
    # @show src
    part
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
    # TODO: Ensure that we handle reductions that can produce nothing
    src = isnothing(src) ? part : op(src, part)

    # # # println("In ReduceAndCopyTo where batch_idx=$batch_idx")
    # @show src
    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, params, Dict(), comm)

        if loc_name != "Memory"
            # We use 1 here so that it is as if we are copying from the head
            # node
            CopyTo(src, src, params, 1, nbatches, comm, loc_name, loc_params)
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
    dim = params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    if part isa Tuple
        newpartdim = length(split_len(part[dim], batch_idx, nbatches, comm))
        indexapply(_ -> newpartdim, part, index = dim)
    else
        length(split_len(part[dim], batch_idx, nbatches, comm))
    end
end

#####################
# Casting functions #
#####################

function Reduce(part, src_params, dst_params, comm)
    # Get operator for reduction
    op = src_params["reducer"]
    op = src_params["with_key"] ? op(src_params["key"]) : op

    # Get buffer to reduce
    # kind, sendbuf = tobuf(part)
    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

    # # @show kind

    # Perform reduction
    # # # println("In Reduce")
    # @show src_params["with_key"] ? op(src_params["key"]) : "no key"
    # @show part
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
    # @show part
    part
end

ReduceWithKey = Reduce

function Rebalance(part, src_params, dst_params, comm)
    # Get the range owned by this worker
    dim = isa_array(part) ? dst_params["key"] : 1
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    len = size(part, dim)
    scannedstartidx = MPI.Exscan(len, +, comm)
    startidx = worker_idx == 1 ? 1 : scannedstartidx + 1
    endidx = startidx + len - 1

    # Get functions for serializing/deserializing
    ser = isa_array(part) ? serialize : Arrow.write
    # TODO: Use JLD for ser/de for arrays
    # TODO: Ensure that we are properly handling intermediate arrays or
    # dataframes that are empty (especially because they may not have their
    # ndims or dtype or schema). We probably are because dataframes that are
    # empty should concatenate properly. We just need to be sure to not expect
    # every partition to know what its schema is. We can however expect each
    # partition of an array to know its ndims.
    de = if isa_array(part)
        x -> deserialize(IOBuffer(x))
    else
        x -> DataFrame(Arrow.Table(x))
    end

    # Construct buffer to send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    # npartitions = nbatches * nworkers
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Vector{Int64} = []
    # @show nworkers
    # @show npartitions
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
        # @show nbyteswritten

    end
    sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), counts)

    # Create buffer for receiving pieces
    # TODO: Refactor the intermediate part starting from there if we add
    # more cases for this function
    # @show counts
    # @show MPI.Comm_size(comm)
    sizes = MPI.Alltoall(MPI.UBuffer(counts, 1), comm)
    recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

    # Perform the shuffle
    MPI.Alltoallv!(sendbuf, recvbuf, comm)

    # Return the concatenated array
    res = cat(
        [
            de(view(recvbuf.data, displ+1:displ+count)) for
            (displ, count) in zip(recvbuf.displs, recvbuf.counts)
        ]...;
        dims = dim,
    )
    # # # println("After rebalancing...")
    # # @show length(res)
    res
end

function Distribute(part, src_params, dst_params, comm)
    # dim = isa_array(part) ? dst_params["key"] : 1
    # TODO: Determine whether copy is needed
    SplitBlock(part, dst_params, 1, 1, comm, "Memory", Dict())
    # part = copy(split_on_executor(part, dim, 1, 1, comm))
    # part
end

function Consolidate(part, src_params, dst_params, comm)
    kind, sendbuf = tobuf(part)
    recvvbuf = buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized
    # if isa_df(part)
    #     io = IOBuffer()
    #     Arrow.write(io, part)
    #     MPI.Allgatherv!()
    # elseif isa_array(part)

    # else
    #     throw(ArgumentError("Expected either array or dataframe to consolidate"))
    # end
    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    part = merge_on_executor(
        kind,
        recvvbuf,
        get_nworkers(comm);
        key = (isa_array(part) ? src_params["key"] : 1),
    )
    part
end

DistributeAndShuffle(part, src_params, dst_params, comm) =
    SplitGroup(part, dst_params, 1, 1, comm, "Memory", Dict())

function Shuffle(
    part,
    src_params,
    dst_params,
    comm;
    boundedlower = false,
    boundedupper = false,
)
    # Get the divisions to apply
    divisions = dst_params["divisions"] # list of min-max tuples
    key = dst_params["key"]
    rev = dst_params["rev"]
    ndivisions = length(divisions)
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    divisions_by_worker = get_divisions(divisions, nworkers) # list of min-max tuple lists
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
    @show typeof(part)
    res = if isa_df(part)
        # Ensure that this partition has a schema that is suitable for usage
        # here. We have to do this for `Shuffle` and `SplitGroup` (which is
        # used by `DistributeAndShuffle`)
        if isempty(part)
            # inv: !(key in names(df))
            part[!, key] = []
        end

        # Compute the partition to send each row of the dataframe to
        transform!(part, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)

        # Group the dataframe's rows by what partition to send to
        gdf = groupby(part, :banyan_shuffling_key, sort = true)

        # Create buffer for sending dataframe's rows to all the partitions
        io = IOBuffer()
        nbyteswritten = 0
        df_counts::Vector{Int64} = []
        for partition_idx = 1:nworkers
            Arrow.write(io, partition_idx in keys(gdf) ? gdf[partition_idx] : part[1:0, :])
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
        res = vcat(
            [
                DataFrame(
                    Arrow.Table(IOBuffer(view(recvbuf.data, displ+1:displ+count))),
                    copycols = false,
                ) for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
            ]...,
        )
        select!(res, Not(:banyan_shuffling_key))

        res
    elseif isa_array(part)
        # Group the data along the splitting axis (specified by the "key"
        # parameter)
        partition_idx_to_e = [[] for partition_idx = 1:nworkers]
        for e in eachslice(part, dims = key)
            partition_idx = get_partition_idx_from_divisions(e, divisions_by_worker)
            push!(partition_idx_to_e[partition_idx], e)
        end

        # Construct buffer for sending data
        io = IOBuffer()
        nbyteswritten = 0
        a_counts::Vector{Int64} = []
        for partition_idx = 1:nworkers
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
        cat(
            [
                deserialize(IOBuffer(view(recvbuf.data, displ+1:displ+count))) for
                (displ, count) in zip(recvbuf.displs, recvbuf.counts)
            ]...;
            dims = key,
        )
    else
        throw(ArgumentError("Expected array or dataframe to distribute and shuffle"))
    end

    # Store divisions
    global splitting_divisions
    splitting_divisions[res] =
        (divisions_by_worker[worker_idx], worker_idx > 1, worker_idx < nworkers)

    res
end
