ReturnNullGrouping(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = nothing

ReturnNullGrouping(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    src = nothing
    src
end

function read_csv_file(path, header, rowrange, readrange, filerowrange, dfs)
    f = CSV.File(
        path,
        header = header,
        skipto = header + readrange.start - filerowrange.start + 1,
        footerskip = filerowrange.stop - readrange.stop,
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

function read_parquet_file(path, header, rowrange, readrange, filerowrange, dfs)
    f = Parquet.read_parquet(
        path,
        rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
    )
    push!(dfs, DataFrames.DataFrame(f, copycols=false))
end

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

            @show loc_params
            @show loc_name
            path = Banyan.getpath(loc_params["path"])

            # Handle multi-file tabular datasets

            # Handle None location by finding all files in directory used for spilling
            # this value to disk
            if loc_name == "Disk"
                # TODO: Only collect files and nrows info for this location associated
                # with a unique name corresponding to the value ID - only if this is
                # the first batch or loop iteration.
                name = loc_params["path"]
                name_path = Banyan.getpath(name)
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
            rowrange = Banyan.split_len(nrows, batch_idx, nbatches, comm)
            dfs::Base.Vector{DataFrames.DataFrame} = []
            rowsscanned = 0
            for file in sort(loc_params["files"], by = filedict -> filedict["path"])
                newrowsscanned = rowsscanned + file["nrows"]
                filerowrange = (rowsscanned+1):newrowsscanned
                # Check if the file corresponds to the range of rows for the batch
                # currently being processed by this worker
                if Banyan.isoverlapping(filerowrange, rowrange)
                    # Deterine path to read from
                    file_path = file["path"]
                    path = Banyan.getpath(file_path)

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
                        read_file(path, header, rowrange, readrange, filerowrange, dfs)
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
            res = if isempty(dfs)
                # Note that if we are reading disk-spilled Arrow data, we would have
                # files for each of the workers that wrote that data. So there should
                # be files but they might be empty.
                if loc_name == "Disk"
                    files_sorted_by_nrow = sort(loc_params["files"], by = filedict -> filedict["nrows"])
                    if isempty(files_sorted_by_nrow)
                        # This should not be empty for disk-spilled data
                        DataFrames.DataFrame()
                    else
                        empty(DataFrames.DataFrame(Arrow.Table(Banyan.getpath(first(files_sorted_by_nrow)["path"])), copycols=false))
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
            println("In ReadBlockCSV with $(nrow(res)) rows")
            res
        end
        ReadBlock
    end
    for (read_file, file_extension) in [
        (read_csv_file, ".csv"),
        (read_parquet_file, ".parquet"),
        (read_arrow_file, ".arrow")
    ]
]

ReadGroupCSV, ReadGroupParquet, ReadGroupArrow = [
    ReadGroup(ReadBlock)
    for ReadBlock in [ReadBlockCSV, ReadBlockParquet, ReadBlockArrow]
]

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
                    Banyan.rmdir_on_nfs(actualpath)
                end

                # Create directory if it doesn't exist
                # TODO: Avoid this and other filesystem operations that would be costly
                # since S3FS is being used
                if batch_idx == 1
                    Banyan.rmdir_on_nfs(path)
                    mkpath(path)
                end
            end
            MPI.Barrier(comm)

            nrows = size(part, 1)
            sortableidx = Banyan.sortablestring(idx, get_npartitions(nbatches, comm))
            write_file(part, path, sortableidx, nrows)
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
                    tmpdir_idx = findfirst(fn -> startswith(fn, "part$idx"), tmpdir)
                    if !isnothing(tmpdir_idx)
                        tmpsrc = joinpath(path, tmpdir[tmpdir_idx])
                        actualdst = joinpath(actualpath, tmpdir[tmpdir_idx])
                        cp(tmpsrc, actualdst, force=true)
                    end
                end
                MPI.Barrier(comm)
                if worker_idx == 1
                    Banyan.rmdir_on_nfs(path)
                end
                MPI.Barrier(comm)
            end
            src
            # TODO: Delete all other part* files for this value if others exist
        end
        Write
    end
    for write_file in [write_parquet_file, write_csv_file, write_arrow_file]
]

CopyFromArrow(src, params, batch_idx, nbatches, comm, loc_name, loc_params) = begin
    params["key"] = 1
    ReadBlockArrow(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyFromCSV(src, params, batch_idx, nbatches, comm, loc_name, loc_params) = begin
    params["key"] = 1
    ReadBlockCSV(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

CopyFromParquet(src, params, batch_idx, nbatches, comm, loc_name, loc_params) = begin
    params["key"] = 1
    ReadBlockParquet(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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
) = if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
    println("In CopyToCSV with $(nrow(part)) rows")
    params["key"] = 1
    WriteCSV(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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
) = if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteParquet(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
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
) = if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
    params["key"] = 1
    WriteArrow(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
end

function Banyan.SplitBlock(
    src::T,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    Banyan.split_on_executor(
        src,
        1,
        batch_idx,
        nbatches,
        comm,
    )
end

function Banyan.SplitGroup(
    src::AbstractDataFrame,
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
    rev = params["rev"]
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
        splitting_divisions = Banyan.get_splitting_divisions()
        splitting_divisions[res] = (
            divisions_by_partition[partition_idx],
            !hasdivision || boundedlower || partition_idx != firstdivisionidx,
            !hasdivision || boundedupper || partition_idx != lastdivisionidx,
        )
    end

    res
end

# It's only with BanyanDataFrames to we have block-partitioned things that can
# be merged to become nothing.
# Grouped data frames can be block-partitioned but we will have to
# redo the groupby if we try to do any sort of merging/splitting on it.
Banyan.Rebalance(
    part::Union{Nothing,GroupedDataFrame},
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) = nothing

function Banyan.Rebalance(
    part::AbstractDataFrame,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
)
    # Get the range owned by this worker
    dim = 1
    worker_idx, nworkers = Banyan.get_worker_idx(comm), Banyan.get_nworkers(comm)
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

# If this is a grouped data frame or nothing (the result of merging
# a grouped data frame is nothing), we consolidate by simply returning
# nothing.
Banyan.Consolidate(part::Union{Nothing, DataFrames.GroupedDataFrame}, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing

function Banyan.Consolidate(part::AbstractDataFrame, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm)
    println("In start of Consolidate with $(nrow(part)) rows")
    io = IOBuffer()
    Arrow.write(io, part)
    sendbuf = MPI.Buffer(view(io.data, 1:io.size))
    recvvbuf = Banyan.buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized

    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    results = [
        view(
            recvvbuf.data,
            (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
        ) |> IOBuffer |> Arrow.Table |> DataFrames.DataFrame
        for i in 1:Banyan.get_nworkers(comm)
    ]
    @show length(results)
    @show nrow.(results)
    res = merge_on_executor(
        [
            view(
                recvvbuf.data,
                (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
            ) |> IOBuffer |> Arrow.Table |> DataFrames.DataFrame
            for i in 1:Banyan.get_nworkers(comm)
        ]...;
        key = 1
    )
    println("In end of Consolidate with $(nrow(res)) rows and io.size=$(io.size), Banyan.get_nworkers(comm)=$(Banyan.get_nworkers(comm)), recvvbuf.counts=$(recvvbuf.counts), recvvbuf.displs=$(recvvbuf.displs), typeof(recvvbuf.data)=$(typeof(recvvbuf.data))")
    res
end

function Shuffle(
    part::AbstractDataFrame,
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
        df_counts::Base.Vector{Int64} = []
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
        splitting_divisions = Banyan.get_splitting_divisions()
        splitting_divisions[res] =
            (divisions_by_worker[worker_idx], !hasdivision || worker_idx != firstdivisionidx, !hasdivision || worker_idx != lastdivisionidx)
    end

    res
end