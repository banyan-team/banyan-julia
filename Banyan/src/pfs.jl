# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pt_lib.jl` should have a corresponding
# `pt_lib_info.json` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

using Serialization
using Base64

using MPI, DataFrames, FilePathsBase, HDF5, CSV, Parquet, Arrow

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

function format_bytes(bytes, decimals = 2)
    bytes == 0 && return "0 Bytes"
    k = 1024
    dm = decimals < 0 ? 0 : decimals
    sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    i = Base.convert(Int, floor(log(bytes) / log(k)))
    return string(round((bytes / ^(k, i)), digits = dm)) * " " * sizes[i+1]
end

format_available_memory() =
    format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

function sortablestring(val, maxval)
    s = string(val)
    maxs = string(maxval)
    res = fill('0', length(maxs))
    res[length(res)-length(s)+1:length(res)] .= collect(s)
    join(res)
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
    # # # # @showpath
    # # # # @showisfile(loc_params["path"])
    # # # # @showHDF5.ishdf5(loc_params["path"])
    println("Reading a block with $path from $loc_name with batch_idx=$batch_idx")
    if (loc_name == "Disk" && HDF5.ishdf5(path)) ||
       (loc_name == "Remote" && (occursin(".h5", loc_params["path"]) || occursin(".hdf5", loc_params["path"])))
        f = h5open(path, "r")
        # # # @showkeys(f)
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
        # end
        # # # # println("In ReadBlock")
        # # # # @showfirst(dset)
        # # @showdset
        return dset
    end
    println("Still in reading a block")

    # # @showloc_name
    # # @showpath
    # # @showisfile(path)
    # # @showisdir(path)
    if isdir(path)
        # # @showreaddir(path)
    end

    # Handle single-file replicated objects
    if loc_name == "Disk" && isfile(path)
        # # # println("In Read")
        # # # @showpath
        res = deserialize(path)
        # # @showres
        return res
    end
    println("Still _still_ in reading a block")

    # Handle multi-file tabular datasets

    # Handle None location by finding all files in directory used for spilling
    # this value to disk
    if loc_name == "Disk"
        name = loc_params["path"]
        # TODO: isdir might not work for S3FS
        if isdir(getpath(name))
            files = []
            nrows = 0
            for partfilename in readdir(getpath(name))
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
            loc_params = Dict{String,Any}(loc_params)
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
    println("Still _still_  *still* in reading a block")

    # Iterate through files and identify which ones correspond to the range of
    # rows for the batch currently being processed by this worker
    @show loc_params
    nrows = loc_params["nrows"]
    rowrange = split_len(nrows, batch_idx, nbatches, comm)
    dfs::Vector{DataFrames.DataFrame} = []
    rowsscanned = 0
    # @showloc_params
    for file in sort(loc_params["files"], by = filedict -> filedict["path"])
        println("Considering $file")
        newrowsscanned = rowsscanned + file["nrows"]
        filerowrange = (rowsscanned+1):newrowsscanned
        # Check if te file corresponds to the range of rows for the batch
        # currently being processed by this worker
        # @showisoverlapping(filerowrange, rowrange)
        # @showrowsscanned
        # @showformat_available_memory()
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
            if endswith(file["path"], ".csv")
                # # # @showisdir("/home/ec2-user/s3fs/")
                # # # @showisdir("/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster02-f47c1c35/")
                # # # @showisfile("/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster02-f47c1c35/iris_large.csv")
                # # # @showisfile("/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster02-f47c1c35/pt_lib.jl")
                # # # @showreaddir("/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster02-f47c1c35/")
                # # # @showpath
                # # # @showisfile(path)
                # f = CSV.File(path)
                # println("Actually reading from $path")
                # # @showisfile(path)
                # # @showformat_bytes(filesize(path))
                # println("Read in directory")
                # f = read(
                #     "/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster03-231c2ef6/iris.csv",
                #     String,
                # )
                # println("At least we read _something_ in with read")
                # f = CSV.File(
                #     "/home/ec2-user/s3fs/banyan-cluster-data-pumpkincluster03-231c2ef6/iris.csv",
                # )
                # println("At least we read _something_ in with CSV.read")
                # f = CSV.File(path, limit = 1)
                # println("Read the first line of the actual file in with CSV.File")
                # # @showlength(filerowrange) length(rowrange) length(readrange)
                # # @showfilerowrange rowrange readrange
                # # @showfile["nrows"]
                # # @showheader
                # # @showheader + readrange.start - filerowrange.start + 1
                # # @showfilerowrange.stop - readrange.stop
                @show path
                @show header
                @show header + readrange.start - filerowrange.start + 1
                @show filerowrange.stop - readrange.stop
                @show isfile(path)
                @show ispath(path)
                @show typeof(path)
                @show isdir("/home/ec2-user/s3fs")
                @show isdir("/home/ec2-user/s3fs/banyan-cluster-data-testcluster01-1e760506")
                @show isfile("/home/ec2-user/s3fs/banyan-cluster-data-testcluster01-1e760506/iris_large.csv")
                f = CSV.File(
                    path,
                    header = header,
                    skipto = header + readrange.start - filerowrange.start + 1,
                    footerskip = filerowrange.stop - readrange.stop,
                )
                println("Finished reading from $path")
                push!(dfs, DataFrames.DataFrame(f))
                # push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
                println("Pushed data frame")
                # buf = Arrow.tobuffer(f)
                # println("Converted to buffer")
                # tbl = Arrow.Table(buf)
                # println("Converted to buffer and to table")
                f = nothing
                GC.gc(true)
                format_available_memory()
            elseif endswith(file["path"], ".parquet")
                f = Parquet.read_parquet(
                    path,
                    rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
                )
                push!(dfs, DataFrames.DataFrame(f))
                # push!(dfs, DataFrame(Arrow.Table(Arrow.tobuffer(f))))
            elseif endswith(file["path"], ".arrow")
                println("Reading from $path on batch $batch_idx")
                rbrowrange = filerowrange.start:(filerowrange.start-1)
                for tbl in Arrow.Stream(path)
                    rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+Tables.rowcount(tbl))
                    # # @showrowrange
                    # # @showrbrowrange
                    # # @showisoverlapping(rbrowrange, rowrange)
                    if isoverlapping(rbrowrange, rowrange)
                        readrange =
                            max(rowrange.start, rbrowrange.start):min(
                                rowrange.stop,
                                rbrowrange.stop,
                            )
                        df = DataFrames.DataFrame(tbl)
                        # # @show(readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1)
                        df = df[
                            (readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1),
                            :,
                        ]
                        # # @showdf[1:min(1, nrow(df)), :]
                        # # # @showlength(df)
                        push!(dfs, df)
                        # TODO: Call GC if the data can't fit in memory
                    end
                end
            else
                error("Expected CSV or Parquet or Arrow format")
            end
        end
        rowsscanned = newrowsscanned
    end
    println("Still _still_  *STILL* in reading a block but after having $(length(dfs)) dfs")

    # # # @showlength(dfs)

    # # @showdfs

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
                empty(Arrow.Table(getpath(first(files_sorted_by_nrow)["path"])) |> DataFrame)
            end
        else
            # When we construct the location, we store an empty data frame with The
            # correct schema.
            from_jl_value_contents(loc_params["emptysample"])
        end
    else
        vcat(dfs...)
    end
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
    @show divisions
    partition_divisions = get_divisions(divisions, npartitions)

    println("In ReadGroup")
    @show divisions partition_divisions

    # TODO: Do some reversing here instead of only doing it later in Shuffle
    # to ensure that sorting in reverse order works correctly

    if batch_idx == 1 && get_worker_idx(comm) == 1
        println("In ReadGroup with divisions=$divisions and partition_divisions=get_divisions(divisions, npartitions)=$partition_divisions")
    end

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
    @show nworkers
    @show nbatches
    for worker_division_idx = 1:nworkers
        for batch_division_idx = 1:nbatches
            # partition_division_idx =
            #     (worker_division_idx - 1) * nbatches + batch_division_idx
            partition_division_idx =
                get_partition_idx(batch_division_idx, nbatches, worker_division_idx)
            @show worker_division_idx batch_division_idx partition_division_idx batch_idx
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

    @show firstbatchidx lastbatchidx firstdivisionidx lastdivisionidx
    @show curr_partition_divisions batch_idx

    if get_worker_idx(comm) == 1
        println("In ReadGroup on batch $batch_idx with curr_partition_divisions=$curr_partition_divisions for shuffling")
    end

    # Read in each batch and shuffle it to get the data for this partition
    parts = []
    for i = 1:nbatches
        # Read in data for this batch
        part = ReadBlock(src, params, i, nbatches, comm, loc_name, loc_params)

        # # @showi nbatches typeof(part)

        # Shuffle the batch and add it to the set of data for this partition
        push!(
            parts,
            Shuffle(
                part,
                Dict(),
                merge(params, Dict("divisions_by_worker" => curr_partition_divisions)),
                comm,
                boundedlower = !hasdivision || batch_idx != firstbatchidx,
                boundedupper = !hasdivision || batch_idx != lastbatchidx,
                store_splitting_divisions = false
            ),
        )
    end

    # # # @showget_worker_idx(comm) parts

    # Concatenate together the data for this partition
    # res = merge_on_executor(parts, dims=isa_array(first(parts)) ? key : 1)
    # # # # @showparts
    res = merge_on_executor(parts...; key = key)
    # # # # @showres

    # If there are no divisions for any of the partitions, then they are all
    # bounded. For a partition to be unbounded on one side, there must be a
    # division(s) for that partition.

    # Store divisions
    global splitting_divisions
    partition_idx = get_partition_idx(batch_idx, nbatches, comm)
    splitting_divisions[res] =
        (partition_divisions[partition_idx], !hasdivision || partition_idx != firstdivisionidx, !hasdivision || partition_idx != lastdivisionidx)

    # # # @showres
    if isa_df(res)
        if key in propertynames(res)
            println("Output of ReadGroup has length $(nrow(res)) with $(unique(res[!, key]))")
        else
            println("Output of ReadGroup has length $(nrow(res)) with no values")
        end
    end
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
    # if batch_idx > 1
    GC.gc()
    # end

    println("Start write")

    # @showpart

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

    # # # println("In Write where batch_idx=$batch_idx")

    # Write file for this partition
    worker_idx = get_worker_idx(comm)
    # println("Writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
    idx = get_partition_idx(batch_idx, nbatches, comm)
    println("In Write on worker $worker_idx on batch $batch_idx")
    # # @showisa_df(part)
    # # @showpart
    if isa_df(part)
        println("Writing data frame with $nbatches batches on batch $batch_idx")
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
            println("Before first barrier in write")
            MPI.Barrier(comm)
            println("After first barrier in write")
        end

        if worker_idx == 1
            if nbatches == 1
                # If there is no batching we can delete the original directory
                # right away. Otherwise, we must delete the original directory
                # only at the end.
                # TODO: When refactoring the locations, think about how to allow
                # stuff in the directory
                rm(actualpath, force = true, recursive = true)
            end

            # Create directory if it doesn't exist
            # TODO: Avoid this and other filesystem operations that would be costly
            # since S3FS is being used
            if batch_idx == 1
                rm(path, force = true, recursive = true)
                mkpath(path)
            end
        end
        println("Before second barrier in write")
        MPI.Barrier(comm)
        println("After second barrier in write")

        nrows = size(part, 1)
        sortableidx = sortablestring(idx, get_npartitions(nbatches, comm))
        if endswith(path, ".parquet")
            # Parquet.jl doesn't support writing empty data frames.
            if nrows > 0
                partfilepath = joinpath(path, "part$sortableidx" * "_nrows=$nrows.parquet")
                Parquet.write_parquet(partfilepath, part)
                println("Wrote to $partfilepath")
            end
        elseif endswith(path, ".csv")
            partfilepath = joinpath(path, "part$sortableidx" * "_nrows=$nrows.csv")
            CSV.write(partfilepath, part)
            println("Wrote to $partfilepath")
        else
            partfilepath = joinpath(path, "part$sortableidx" * "_nrows=$nrows.arrow")
            println("Going to write to $partfilepath")
            Arrow.write(partfilepath, part)
            println("Wrote to $partfilepath")
        end
        MPI.Barrier(comm)
        if nbatches > 1 && batch_idx == nbatches
            tmpdir = readdir(path)
            if worker_idx == 1
                rm(actualpath, force = true, recursive = true)
                mkpath(actualpath)
            end
            println("Created $actualpath")
            MPI.Barrier(comm)
            # # # @showpath tmpdir get_nworkers(comm) nbatches
            for batch_i = 1:nbatches
                idx = get_partition_idx(batch_i, nbatches, worker_idx)
                tmpdir_idx = findfirst(fn -> startswith(fn, "part$idx"), tmpdir)
                if !isnothing(tmpdir_idx)
                    tmpsrc = joinpath(path, tmpdir[tmpdir_idx])
                    actualdst = joinpath(actualpath, tmpdir[tmpdir_idx])
                    cp(tmpsrc, actualdst)
                    println("Copied $tmpsrc to $actualdst")
                end
            end
            MPI.Barrier(comm)
            # TODO: Maybe somehow flush the above or fsync the directory
            if worker_idx == 1
                rm(path, force = true, recursive = true)
            end
            MPI.Barrier(comm)
            println("Removed temporary $path")
        end
        println("Finished writing data frame")
        src
        # TODO: Delete all other part* files for this value if others exist
    elseif isa_array(part) && hasmethod(HDF5.datatype, (eltype(part),))
        # TODO: Use Julia serialization to write arrays as well as other
        # objects to disk. This way, we won't trip up when we come across
        # a distributed array that we want to write to disk but can't because
        # of an unsupported HDF5 data type.
        # TODO: Support missing values in the array for locations that use
        # Julia serialized objects
        part = DataFrames.disallowmissing(part)

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
            # # # @showoffset

            # Create file if not yet created
            # # # @showpath
            # TODO: Figure out why sometimes a deleted file still `isfile`
            # # # @showisfile(path) # This is true while below is false
            # # # @showHDF5.ishdf5(path)
            # NOTE: We used INDEPENDENT here on the most recent run
            # f = h5open(path, "cw", fapl_mpio=(comm, info), dxpl_mpio=HDF5.H5FD_MPIO_INDEPENDENT)
            f = h5open(
                path,
                "cw",
                fapl_mpio = (comm, info),
                dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
            )
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
            whole_size = indexapply(+, size(part), offset, index = dim)
            whole_size = MPI.bcast(whole_size, nworkers - 1, comm) # Broadcast dataset size to all workers
            # # # @showeltype(part)
            # # # @showwhole_size
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
            #     # # # @showgroup
            #     # # # @shownbatches
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
            #         # # # @showkeys(fid)
            #         # # # @showfid
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
            fsync_file(p) =
                open(p) do f
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
            #     #         # # # @showgroup_prefix
            #     #         # # # @showkeys(f)
            #     #         delete_object(f[group_prefix])
            #     #         # # # @showkeys(f)
            #     #     end
            #     # end

            #     # Create the file
            #     h5open(path, dataset_writing_permission) do fid end
            # end
            # # # # println("Entering if")
            # # # # @showgroup
            # # # # @shownbatches
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
            #         # # # @showkeys(fid)
            #         # # # @showfid
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
                # @show"LOOKING HERE NOW"
                # @showpath
                # @showisfile(path)
                # @showHDF5.ishdf5(path)
                # @showHDF5.has_parallel()
                f = h5open(
                    path,
                    "cw",
                    fapl_mpio = (comm, info),
                    dxpl_mpio = HDF5.H5FD_MPIO_COLLECTIVE,
                )
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
            # # # @showpart_lengths
            partdsets = [
                begin
                    idx = get_partition_idx(batch_idx, nbatches, worker_i)
                    group = group_prefix * "_part$idx" * "_dim=$dim"
                    # # # @showkeys(f)
                    # If there are multiple batches, each batch just gets written
                    # to its own group
                    # # # @showindexapply(_ -> part_length, size(part), index=dim)
                    dataspace_size =
                        indexapply(_ -> part_length, size(part), index = dim)
                    new_dset = create_dataset(
                        f,
                        group,
                        eltype(part),
                        (dataspace_size, dataspace_size),
                    )
                    # # # @showeltype(part)
                    # # # # @showkeys(fid)
                    # # # # @showfid
                    # close(fid[group])
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
                        group = group_prefix * "_part$idx" * "_dim=$dim"
                        f[group]
                    end for batch_idx = 1:nbatches
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
                # # # @showgroup
                # # # @shownbatches
                whole_size =
                    indexapply(_ -> offset + whole_batch_length, size(part), index = dim)
                whole_size = MPI.bcast(whole_size, nworkers - 1, comm) # Broadcast dataset size to all workers
                # # # @showwhole_size
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
                # # # @showgroup_prefix
                # # # @showkeys(f)
                dset =
                    create_dataset(f, group_prefix, eltype(part), (whole_size, whole_size))
                # # # @showkeys(f)
                # # # # @showkeys(fid)
                # # # # @showfid
                # close(fid[group_prefix])
                # # # println("Exiting if")

                # Wait until all workers have the file
                # TODO: Maybe use a broadcast so that each node is only blocked on
                # the last node which is where the file is creating
                # TODO: Try removing this barrier
                MPI.Barrier(comm)

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after creating final dataset with available memory: $(format_available_memory())")

                # Write out each batch
                # # # @showHDF5.ishdf5(path)
                # # # @showkeys(f)
                # dset = f[group_prefix]
                batchoffset = offset
                for batch_i = 1:nbatches
                    partdset = partdsets[batch_i]

                    # Determine what index partition this batch is
                    idx = get_partition_idx(batch_i, nbatches, comm)

                    # Write
                    group = group_prefix * "_part$idx" * "_dim=$dim"
                    # # # # @show(batchoffset+1):batchoffset+size(f[group], dim)
                    # # # # @showsize(dset)
                    # # # # @showsize(f[group][fill(Colon(), ndims(dset))...])
                    # # # # @showeltype(dset)
                    # # # # @showeltype(f[group])
                    # # # # @showHDF5.ishdf5(path)
                    # # # # @showkeys(f)
                    # # # # @showsum(f[group][fill(Colon(), ndims(dset))...])
                    # # # @show(batchoffset+1):batchoffset+size(partdset, dim)
                    # # # @showsize(dset)
                    # # # @showsize(partdset[fill(Colon(), ndims(dset))...])
                    # # # @showeltype(dset)
                    # # # @showeltype(partdset)
                    # # # @showHDF5.ishdf5(path)
                    # # # @showkeys(f)
                    # # # @showsum(partdset[fill(Colon(), ndims(dset))...])
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
                    # # # @showsum(getindex(dset, [
                    #     if d == dim
                    #         (batchoffset+1):batchoffset+size(partdset, dim)
                    #     else
                    #         Colon()
                    #     end
                    #     # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
                    #     for d = 1:ndims(dset)
                    # ]...))

                    # # # # @showsize(dset)

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
                # # # # @showsum(dset[fill(Colon(), ndims(dset))...])
                for worker_i = 1:nworkers
                    for batch_i = 1:nbatches
                        idx = get_partition_idx(batch_i, nbatches, worker_i)
                        group = group_prefix * "_part$idx" * "_dim=$dim"
                        # # # println("Deleting a group")
                        # # # @showgroup
                        # # # @showkeys(f)
                        delete_object(f[group])
                        # # # @showkeys(f)
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
                # # # # @showworker_idx
                # MPI.Barrier(comm)
                # # # # @showworker_idx
                # end
                # # # @showworker_idx
                MPI.Barrier(comm)
                # # # @showworker_idx 

                # # println("In writing worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches: after deleting all objects with available memory: $(format_available_memory())")
            end
            close(f)
            f = nothing
            # TODO: Ensure that we are closing stuff everywhere before trying
            # to write
            # # # @showworker_idx
            MPI.Barrier(comm)
            # # # @showworker_idx 

            # # Allocate all datasets needed by gathering all sizes to the head
            # # node and making calls from there
            # part_lengths = MPI.Gather(size(part, dim), 0, comm)
            # if worker_idx == 1
            #     h5open(path, "r+") do fid
            #         for (worker_i, part_length) in enumerate(part_lengths)
            #             idx = get_partition_idx(batch_idx, nbatches, worker_i)
            #             group = group_prefix*"_part$idx"*"_dim=$dim"
            #             # # # @showkeys(fid)
            #             # If there are multiple batches, each batch just gets written
            #             # to its own group
            #             # TODO: Use `dataspace` instead of similar since the array
            #             # may not fit in memory'
            #             # # # @showindexapply(_ -> part_length, size(part), index=dim)
            #             fid[group] = similar(part, indexapply(_ -> part_length, size(part), index=dim))
            #             # # # @showeltype(part)
            #             # # # # @showkeys(fid)
            #             # # # # @showfid
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
            #     # # # # @showkeys(fid)
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
            #         # # # @showgroup
            #         # # # @shownbatches
            #         whole_size = indexapply(_ -> offset + whole_batch_length, size(part), index=dim)
            #         # # # @showwhole_size
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
            #             # # # @showgroup_prefix
            #             # # # @showkeys(fid)
            #             fid[group_prefix] = similar(part, whole_size)
            #             # # # @showkeys(fid)
            #             # # # # @showkeys(fid)
            #             # # # # @showfid
            #             close(fid[group_prefix])
            #         end
            #         # # # println("Exiting if")
            #     end

            #     # Wait until all workers have the file
            #     # TODO: Maybe use a broadcast so that each node is only blocked on
            #     # the last node which is where the file is creating
            #     MPI.Barrier(comm)

            #     # Write out each batch
            #     # # # @showHDF5.ishdf5(path)
            #     h5open(path, "r+") do f
            #         # # # @showkeys(f)
            #         dset = f[group_prefix]
            #         batchoffset = offset
            #         for batch_i = 1:nbatches
            #             # Determine what index partition this batch is
            #             idx = get_partition_idx(batch_i, nbatches, comm)

            #             # Write
            #             group = group_prefix*"_part$idx"*"_dim=$dim"
            #             # # # @show(batchoffset+1):batchoffset+size(f[group], dim)
            #             # # # @showsize(dset)
            #             # # # @showsize(f[group][fill(Colon(), ndims(dset))...])
            #             # # # @showeltype(dset)
            #             # # # @showeltype(f[group])
            #             # # # @showHDF5.ishdf5(path)
            #             # # # @showkeys(f)
            #             # # # @showsum(f[group][fill(Colon(), ndims(dset))...])
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
            #             # # # @showsum(getindex(dset, [
            #                 if d == dim
            #                     (batchoffset+1):batchoffset+size(f[group], dim)
            #                 else
            #                     Colon()
            #                 end
            #                 # split_len(whole_size[dim], batch_idx, nbatches, comm) : Colon()
            #                 for d = 1:ndims(dset)
            #             ]...))

            #             # # # # @showsize(dset)

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
            #             # # # @showsum(f[group_prefix][fill(Colon(), ndims(f[group_prefix]))...])
            #             for worker_i = 1:nworkers
            #                 for batch_i = 1:nbatches
            #                     idx = get_partition_idx(batch_i, nbatches, worker_i)
            #                     group = group_prefix*"_part$idx"*"_dim=$dim"
            #                     # # # println("Deleting a group")
            #                     # # # @showgroup
            #                     # # # @showkeys(f)
            #                     delete_object(f[group])
            #                     # # # @showkeys(f)
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

        # # # @showpath
        # # # @showHDF5.ishdf5(path)

        # Wait till the dataset is created
        # # # # println("Starting to wait")

        # Write part to the dataset
        # # # # @showf
        # # # # @showkeys(f)
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
        # # # # @showsize(dset, dim)
        # # # # @showbatch_idx
        # # # # @shownbatches
        # # # # @showwhole_size
        # # # # @showsplit_len(whole_size[dim], batch_idx, nbatches, comm)
        # # # # @showsize(dset)
        # # # # @showsize(part)

        # dsubset = split_on_executor(dset, dim, batch_idx, nbatches, comm)
        # NOTE: For writing to HDF5 datasets we can't just use
        # split_on_executor because we aren't reading a copy; instead, we are
        # writing to a slice
        # # # # @showfirst(part)
        # # # # @showfirst(dset[:])
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
        # # # @showpart
        # # # @showpath
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
    println("haskey(splitting_divisions, src)=$(haskey(splitting_divisions, src))")
    src_divisions, boundedlower, boundedupper = get!(splitting_divisions, src) do
        # This case lets us use `SplitGroup` in `DistributeAndShuffle`
        (params["divisions"], false, false)
    end
    divisions_by_partition = get_divisions(src_divisions, npartitions)

    println("In SplitGroup")
    @show src_divisions divisions_by_partition boundedlower boundedupper

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

    println("In SplitGroup")
    @show divisions_by_partition

    # Apply divisions to get only the elements relevant to this worker
    # # # # @showkey
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
        # # # @showbatch_idx worker_idx src res
    end

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
    # TODO: Ensure we can merge grouped dataframes if computing them

    global partial_merges
    global splitting_divisions

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
        println("In Merge")
        # # # @showsrc part
        if batch_idx == 1 || batch_idx == nbatches
            # println("At start of merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory()) and used: $(format_bytes(Base.summarysize(src)))")
        end
        if batch_idx == nbatches
            # println("At start of merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
            delete!(partial_merges, objectid(src))
            delete!(splitting_divisions, part)
            # # # @showworker_idx batch_idx src

            println("On last batch of merging on worker $worker_idx")
            # # @showsrc

            # TODO: Test that this merges correctly
            # src = merge_on_executor(src...; dims=dim)
            src = merge_on_executor(src...; key = key)
            # println("After locally merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
            # # @showsrc

            # Concatenate across workers
            nworkers = get_nworkers(comm)
            if nworkers > 1
                src = Consolidate(src, params, Dict(), comm)
                # println("After distributing and merging worker_idx=$worker_idx, batch_idx=$batch_idx/$nbatches with available memory: $(format_available_memory())")
            end
            if is_debug_on()
                # # # @showpartial_merges
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
    # # # @showloc_name
    println("In CopyFrom with loc_name=$loc_name and loc_params=$loc_params")
    # @showloc_params
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
        params = Dict{String,Any}(params)
        params["key"] = 1
        res = ReadBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        # # # println("In CopyFrom")
        # # # # @showlength(res)
        # # # @showres
        println("At end of CopyFrom")
        res
    elseif loc_name == "Remote"
        params = Dict{String,Any}(params)
        params["key"] = 1
        res = ReadBlock(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
        println("At end of CopyFrom")
        res
    elseif loc_name == "Client"
        received = get_worker_idx(comm) == 1 ? receive_from_client(loc_params["value_id"]) : nothing
        received = MPI.bcast(received, 0, comm)
        println("In CopyFrom Client")
        # # @showreceived
        @show received
        received
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
    println(
        "In CopyTo with loc_name=$loc_name and loc_params=$loc_params and worker_idx=$(get_worker_idx(comm)) and batch_idx=$batch_idx",
    )
    # # # println("In CopyTo")
    # # # @showget_partition_idx(batch_idx, nbatches, comm)
    # # # @showget_npartitions(nbatches, comm)
    # # # @showget_worker_idx(comm)
    # # # @showMPI.Comm_rank(MPI.COMM_WORLD)
    # # # @showpart
    # # # @showloc_name
    if loc_name == "Memory"
        src = part
    else
        params = Dict{String,Any}(params)
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
            # # # @showpart
            if get_partition_idx(batch_idx, nbatches, comm) == 1
                send_to_client(loc_params["value_id"], part)
            end
            println("Before barrier in sending to client")
            # TODO: Remove this barrier if not needed to ensure correctness
            MPI.Barrier(comm)
            println("After barrier in sending to client")
        else
            error("Unexpected location")
        end
    end
    # # # @showsrc
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
    # # # @showsrc
    # Merge reductions across workers
    # # # @showsrc part params["with_key"]
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

    # # # # @showkind

    # Perform reduction
    # # # println("In Reduce")
    # # # @showsrc_params["with_key"] ? op(src_params["key"]) : "no key"
    # # # @showpart
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
    # # # @showpart
    part
end

ReduceWithKey = Reduce

function Rebalance(part, src_params, dst_params, comm)
    println("At start of Rebalance")
    # @showpart

    if isnothing(part) || isa_gdf(part)
        # Grouped data frames can be block-partitioned but we will have to
        # redo the groupby if we try to do any sort of merging/splitting on it.
        return nothing
    end

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
        x -> DataFrames.DataFrame(Arrow.Table(IOBuffer(x)))
    end

    # Construct buffer to send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    # npartitions = nbatches * nworkers
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Vector{Int64} = []
    # # # @shownworkers
    # # # @shownpartitions
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
        # # # @shownbyteswritten

    end
    sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), counts)

    # Create buffer for receiving pieces
    # TODO: Refactor the intermediate part starting from there if we add
    # more cases for this function
    # # # @showcounts
    # # # @showMPI.Comm_size(comm)
    sizes = MPI.Alltoall(MPI.UBuffer(counts, 1), comm)
    recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

    # Perform the shuffle
    MPI.Alltoallv!(sendbuf, recvbuf, comm)

    # Return the concatenated array
    things_to_concatenate = [
        de(view(recvbuf.data, displ+1:displ+count)) for
        (displ, count) in zip(recvbuf.displs, recvbuf.counts)
    ]
    # @showthings_to_concatenate
    # @showdim
    res = merge_on_executor(
        things_to_concatenate...;
        key = dim,
    )
    # # # println("After rebalancing...")
    # # # # @showlength(res)
    println("At end of Rebalance")
    # @showres
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
    if isnothing(part) || isa_gdf(part)
        # If this is a grouped data frame or nothing (the result of merging
        # a grouped data frame is nothing), we consolidate by simply returning
        # nothing.
        return nothing
    end
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
    if isnothing(part)
        println("Output of Consolidate is nothing")
    end
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

    println("In Shuffle on worker_idx=$worker_idx with boundedlower=$boundedlower and boundedupper=$boundedupper")

    # Perform shuffle
    # # @showdivisions
    # # @showdivisions_by_worker
    # # @showorderinghash("setosa")
    # # @showorderinghash("versicolor")
    # # @showorderinghash("virginica")
    # # @showboundedlower
    # # @showboundedupper
    partition_idx_getter(val) = get_partition_idx_from_divisions(
        val,
        divisions_by_worker,
        boundedlower = boundedlower,
        boundedupper = boundedupper,
    )
    # # # @showtypeof(part)
    res = if isa_df(part)   
        # # Ensure that this partition has a schema that is suitable for usage
        # # here. We have to do this for `Shuffle` and `SplitGroup` (which is
        # # used by `DistributeAndShuffle`)
        # if isempty(part)
        #     # inv: !(key in names(df))
        #     part[!, key] = []
        # end

        gdf = if !isempty(part)
            # Compute the partition to send each row of the dataframe to
            DataFrames.transform!(part, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)
            # # @showworker_idx part

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
        res = vcat(
            [
                DataFrames.DataFrame(
                    Arrow.Table(IOBuffer(view(recvbuf.data, displ+1:displ+count))),
                    copycols = false,
                ) for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
            ]...,
        )
        if :banyan_shuffling_key in propertynames(res)
            DataFrames.select!(res, Not(:banyan_shuffling_key))
        end

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
