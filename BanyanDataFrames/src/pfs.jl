ReturnNullGrouping(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

ReturnNullGrouping(
    src,
    part,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = begin
    src = nothing
    src
end

ReturnNullGroupingConsolidated(part, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing
ReturnNullGroupingRebalanced(part, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing

de(x) = DataFrames.DataFrame(Arrow.Table(IOBuffer(x)))

function ShuffleDataFrameHelper(
    part::DataFrames.DataFrame,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm,
    boundedlower::Bool,
    boundedupper::Bool,
    store_splitting_divisions::Bool,
    key::K,
    rev::Bool,
    divisions_by_worker::Base.Vector{Base.Vector{Division{V}}},
    divisions::Base.Vector{Division{V}},
    splitting_divisions::IdDict{Any,Any}
)::DataFrames.DataFrame where {K,V}
    # We don't have to worry about grouped data frames since they are always
    # block-partitioned.

    # Get the divisions to apply
    worker_idx, nworkers = Banyan.get_worker_idx(comm), Banyan.get_nworkers(comm)

    # Perform shuffle
    partition_idx_getter(val) = Banyan.get_partition_idx_from_divisions(
        val,
        divisions_by_worker,
        boundedlower,
        boundedupper,
    )
    res = begin
        gdf::DataFrames.GroupedDataFrame = if !isempty(part)
            # Compute the partition to send each row of the dataframe to
            DataFrames.transform!(part, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)

            # Group the dataframe's rows by what partition to send to
            DataFrames.groupby(part, :banyan_shuffling_key, sort = true)
        else
            DataFrames.groupby(DataFrames.DataFrame(:x => Int64[]), :x)
        end

        # Create buffer for sending dataframe's rows to all the partitions
        io = IOBuffer()
        nbyteswritten::Int64 = 0
        df_counts::Base.Vector{Int64} = Int64[]
        for partition_idx = 1:nworkers
            Arrow.write(
                io,
                if gdf.ngroups > 0 && haskey(gdf, (banyan_shuffling_key = partition_idx,))
                    gdf[(banyan_shuffling_key = partition_idx,)]
                else
                    empty_res = empty(part)
                    empty_res.banyan_shuffling_key = Int64[]
                    empty_res
                end,
                compress=:zstd
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
        res::DataFrames.DataFrame = if length(recvbuf.counts) == 1
            DataFrames.DataFrame(
                Arrow.Table(IOBuffer(view(recvbuf.data, :))),
                copycols = false,
            )
        else
            vcat(
                (
                    DataFrames.DataFrame(
                        Arrow.Table(IOBuffer(view(recvbuf.data, displ+1:displ+count))),
                        copycols = false,
                    ) for (displ, count) in zip(recvbuf.displs, recvbuf.counts)
                )...
            )
        end
        if hasproperty(res, :banyan_shuffling_key)
            DataFrames.select!(res, Not(:banyan_shuffling_key))
        end

        println("In ShuffleDataFrame on get_worker_idx()=$(get_worker_idx()) with nrow(res)=$(DataFrames.nrow(res)) and divisions_by_worker=$divisions_by_worker with boundedlower=$boundedlower and boundedupper=$boundedupper")

        res
    end

    if store_splitting_divisions
        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any(isnotempty, divisions_by_worker)
        firstdivisionidx = findfirst(isnotempty, divisions_by_worker)
        lastdivisionidx = findlast(isnotempty, divisions_by_worker)

        # Store divisions
        splitting_divisions[res] =
            (divisions_by_worker[worker_idx], !hasdivision || worker_idx != firstdivisionidx, !hasdivision || worker_idx != lastdivisionidx)
    end

    res
end


function ShuffleDataFrame(
    part::Union{DataFrames.DataFrame,Empty},
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm,
    boundedlower::Bool = false,
    boundedupper::Bool = false,
    store_splitting_divisions::Bool = true
)
    divisions = deepcopy(dst_params["divisions"])
    has_divisions_by_worker = haskey(dst_params, "divisions_by_worker")
    V = if !isempty(divisions)
        typeof(divisions[1][1])
    elseif has_divisions_by_worker
        typeof(divisions_by_worker[1][1][1])
    else
        Any
    end
    ShuffleDataFrameHelper(
        part,
        src_params,
        dst_params,
        comm,
        boundedlower,
        boundedupper,
        store_splitting_divisions,
        dst_params["key"],
        get(dst_params, "rev", false),
        # Base.Vector{Division{V}}[]
        has_divisions_by_worker ? dst_params["divisions_by_worker"] : Banyan.get_divisions(divisions, get_nworkers(comm)),
        divisions,
        Banyan.get_splitting_divisions()
    )
end

symbol_Disk = "Disk"
symbol_filtering_op = "filtering_op"
symbol_path = "path"
symbol_balanced = "balanced"
symbol_nrows = "nrows"

function ReadBlockHelper(@nospecialize(format_value))
    function ReadBlock(
        src,
        params::Dict{String,Any},
        batch_idx::Int64,
        nbatches::Int64,
        comm::MPI.Comm,
        loc_name::String,
        loc_params::Dict{String,Any},
    )::DataFrames.DataFrame
        # TODO: Implement a Read for balanced=false where we can avoid duplicate
        # reading of the same range in different reads

        if Banyan.INVESTIGATING_LOSING_DATA
            println("In ReadBlock with loc_params=$loc_params params=$params")
        end
        
        loc_params_path = loc_params[symbol_path]::String
        balanced = params[symbol_balanced]
        # By calling getpath we ensure that this data exists on each node and
        # is ready to be read in even if the cluster has changed but same S3 bucket
        # with cached location is used.
        existing_path = getpath(loc_params_path)
        @show loc_params
        meta_path = loc_name == symbol_Disk ? sync_across(is_main_worker(comm) ? get_meta_path(loc_params_path) : "", comm=comm) : loc_params["meta_path"]::String
        @show meta_path
        loc_params = loc_name == symbol_Disk ? (Banyan.deserialize_retry(get_location_path(loc_params_path))::Location).src_parameters : loc_params
        meta = Arrow_Table_retry(meta_path)
        filtering_op = get(params, symbol_filtering_op, identity)

        # Handle multi-file tabular datasets

        time_key = loc_name == symbol_Disk ? (:reading_lustre) : (:reading_remote)

        # TODO: Use split_across to split up the list of files. Then, read in all the files and concatenate them.
        # Finally, shuffle by sending to eahc
        # TODO: use path and nrows
        # Use first-fit-decreasing bin-packing [1] to assign files to workers
        # [1] https://en.wikipedia.org/wiki/First-fit-decreasing_bin_packing

        # Initialize
        meta_nrows = meta.nrows
        meta_path = meta.path
        nworkers = get_nworkers(comm)
        npartitions = nbatches * nworkers
        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        nrows::Int64 = loc_params[symbol_nrows]::Int64
        rows_per_partition = cld(nrows, npartitions)
        sorting_perm = sortperm(meta_nrows, rev=true)
        files_by_partition = Base.Vector{Int64}[]
        nrows_by_partition = Base.zeros(npartitions)
        for _ in 1:npartitions
            push!(files_by_partition, Int64[])
        end

        if Banyan.INVESTIGATING_FILE_PARTITION_PACKING && partition_idx == 1
            @show nrows
            @show rows_per_partition
            @show meta_nrows
            @show meta_path
        end

        # Try to fit as many files as possible into each partition and keep
        # track of the files that are too big
        too_large_files = Int64[]
        for file_i in sorting_perm
            too_large_file = true
            for (i, (files, nrows)) in enumerate(zip(files_by_partition, nrows_by_partition))
                file_nrows = meta_nrows[file_i]
                if nrows + file_nrows <= rows_per_partition
                    push!(files, file_i)
                    nrows_by_partition[i] += file_nrows
                    too_large_file = false # it actually fits!!
                    break
                end
            end

            if too_large_file
                push!(too_large_files, file_i)
            end
        end

        if Banyan.INVESTIGATING_FILE_PARTITION_PACKING && partition_idx == 1
            @show files_by_partition
            @show too_large_files
        end

        # Fit in the files that are too large by first only using partitions
        # that haven't yet been assigned any rows. Prioritize earlier batches.
        second_pass = false
        while !isempty(too_large_files)
            # On the first pass, we try to fit into partitions not yet assigned any file
            # On the second pass, we just assign stuff anywhere
            for batch_i in 1:nbatches
                for worker_i in 1:nworkers
                    curr_partition_idx = get_partition_idx(batch_i, nbatches, worker_i)
                    if (nrows_by_partition[curr_partition_idx] == 0 || second_pass) && !isempty(too_large_files)
                        file_i = popfirst!(too_large_files)
                        push!(
                            files_by_partition[curr_partition_idx],
                            file_i
                        )
                        nrows_by_partition[curr_partition_idx] += meta_nrows[file_i]
                    end
                end
            end
            second_pass = true
        end

        if Banyan.INVESTIGATING_FILE_PARTITION_PACKING && partition_idx == 1
            @show files_by_partition
        end

        # @show files_by_partition

        # Read in data frames
        if !balanced
            files_for_curr_partition = files_by_partition[partition_idx]
            times = Base.Vector{Float64}(undef, length(files_for_curr_partition))
            files_memory_usage = Base.Vector{String}(undef, length(files_for_curr_partition))
            dfs = if !isempty(files_for_curr_partition)
                dfs_res::Base.Vector{DataFrames.DataFrame} = Base.Vector{DataFrames.DataFrame}(undef, length(files_for_curr_partition))
                Threads.@threads for (i, file_i) in Base.collect(enumerate(files_for_curr_partition))
                    path = meta_path[file_i]
                    et = @elapsed begin
                    res = filtering_op(read_file(format_value, path))
                    end
                    dfs_res[i] = res
                    times[i] = et
                    files_memory_usage[i] = Banyan.format_bytes(Banyan.total_memory_usage(res))
                end
                for et in times
                    record_time(time_key, et)
                end
                dfs_res
            else
                DataFrames.DataFrame[]
            end
        else
            # Determine the range of rows to read in from each file so that the result
            # is perfectly balanced across all partitions
            rowrange = Banyan.split_len(nrows, batch_idx, nbatches, comm)
            ndfs = 0
            rowsscanned = 0
            files_to_read = []
            for file in Tables.rows(meta)
                path = file[1]
                path_nrows = file[2]
                newrowsscanned = rowsscanned + path_nrows
                filerowrange = (rowsscanned+1):newrowsscanned

                # Check if the file corresponds to the range of rows for the batch
                # currently being processed by this worker
                if Banyan.isoverlapping(filerowrange, rowrange)
                    ndfs += 1
                    readrange =
                        max(rowrange.start, filerowrange.start):min(
                            rowrange.stop,
                            filerowrange.stop,
                        )
                    push!(files_to_read, (ndfs, path, readrange, filerowrange))
                end
                rowsscanned = newrowsscanned
            end
            dfs = Base.Vector{Any}(undef, ndfs)

            # Iterate through files and identify which ones correspond to the range of
            # rows for the batch currently being processed by this worker
            rowrange = Banyan.split_len(nrows, batch_idx, nbatches, comm)
            rowsscanned = 0
            Threads.@threads for (i, path::String, readrange, filerowrange) in files_to_read
                # TODO: Scale the memory usage appropriately when splitting with
                # this and garbage collect if too much memory is used.
                if Banyan.INVESTIGATING_LOSING_DATA
                    println("In ReadBlock calling read_file with path=$path, filerowrange=$filerowrange, readrange=$readrange, rowrange=$rowrange")
                end
                @time begin
                et = @elapsed begin
                res = read_file(format_value, path, rowrange, readrange, filerowrange)
                dfs[i] = res
                end
                record_time(time_key, et)
                # push!(files_memory_usage, Banyan.format_bytes(Banyan.total_memory_usage(res)))
                println("Time to read $(length(readrange)) rows from file with $(length(filerowrange)) rows with Banyan.total_memory_usage(res)=$(Banyan.total_memory_usage(res)) and filesize(path)=$(Banyan.format_bytes(filesize(path))) from path=$path on get_worker_idx(comm)=$(get_worker_idx(comm)) and batch_idx=$batch_idx = $et seconds for $(Banyan.format_bytes(round(Int64, filesize(path) / et))) per second on get_worker_idx()=$(MPI.Initialized() ? get_worker_idx() : -1)")
                end
            end
        end

        if Banyan.INVESTIGATING_LOSING_DATA
            # println("In ReadBlock with rowrange=$rowrange, nrow.(dfs)=$(nrow.(dfs))")
            println("In ReadBlock with nrow.(dfs)=$(nrow.(dfs))")
        end

        # Concatenate and return
        # NOTE: If this partition is empty, it is possible that the result is
        # schemaless (unlike the case with HDF5 where the resulting array is
        # guaranteed to have its ndims correct) and so if a split/merge/cast
        # function requires the schema (for example for grouping) then it must be
        # sure to take that account
        if isempty(dfs)
            println("No dfs to read in on get_worker_idx()=$(MPI.Initialized() ? get_worker_idx() : -1)")
        else
            println("Time to read $loc_name so far = $(get_time(time_key)) seconds; $(length(dfs)) files read in on get_worker_idx()=$(MPI.Initialized() ? get_worker_idx() : -1)")
        end
        res = if isempty(dfs)
            # When we construct the location, we store an empty data frame with The
            # correct schema.
            from_jl_value_contents(loc_params["empty_sample"])
        elseif length(dfs) == 1
            dfs[1]
        else
            vcat(dfs...)
        end
        @show ncol(res)
        res
    end
    ReadBlock
end

# We currently don't expect to ever have Empty dataframes. We only expect Empty arrays
# and values resulting from mapslices or reduce. If we do have Empty dataframes arising
# that can't just be empty `DataFrame()`, then we will modify functions in this file to
# support Empty inputs.

function WriteHelper(@nospecialize(format_value))
    function Write(
        src,
        part::Union{DataFrames.AbstractDataFrame,Empty},
        params::Dict{String,Any},
        batch_idx::Int64,
        nbatches::Int64,
        comm::MPI.Comm,
        loc_name::String,
        loc_params::Dict{String,Any},
    )
        # Get rid of splitting divisions if they were used to split this data into
        # groups
        splitting_divisions = Banyan.get_splitting_divisions()
        delete!(splitting_divisions, part)

        ###########
        # Writing #
        ###########

        # Get path of directory to write to
        is_disk = loc_name == "Disk"
        loc_params_path = loc_params["path"]::String
        path::String = loc_params_path
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
        is_main = worker_idx == 1
        nworkers = get_nworkers(comm)
        idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
        actualpath = deepcopy(path)
        format_string = file_ending(format_value)
        if nbatches > 1
            # Add _tmp to the end of the path
            path = loc_name == "Disk" ? path * "_tmp" : replace(path, ".$format_string" => "_tmp.$format_string")
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

        if is_main
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

        # Write out for this batch
        nrows = part isa Empty ? 0 : size(part, 1)
        npartitions = get_npartitions(nbatches, comm)
        sortableidx = Banyan.sortablestring(idx, npartitions)
        part_res = part isa Empty ? part : convert(DataFrames.DataFrame, part)
        if !(part isa Empty)
            @time begin
            et = @elapsed begin
            dst = joinpath(path, "part_$sortableidx" * ".$format_string")
            write_file(
                format_value,
                part_res,
                dst,
                nrows
            )
            end
            record_time(:writing, et)
            println("Time to write so far = $(get_time(:writing)) seconds")
            println("Time to write with Banyan.total_memory_usage(part_res)=$(Banyan.format_bytes(Banyan.total_memory_usage(part_res))) and filesize(dst)=$(Banyan.format_bytes(filesize(dst))) to dst=$dst on worker_idx=$worker_idx and batch_idx=$batch_idx = $et seconds for $(Banyan.format_bytes(round(Int64, filesize(dst) / et))) per second")
            end
        end 
        # We don't need this barrier anymore because we do a broadcast right after
        # MPI.Barrier(comm)

        ##########################################
        # SAMPLE/METADATA COLLECTIOM AND STORAGE #
        ##########################################

        # Get paths for reading in metadata and Location
        tmp_suffix = nbatches > 1 ? ".tmp" : ""
        meta_path = is_main ? get_meta_path(loc_params_path * tmp_suffix) : ""
        location_path = is_main ? get_location_path(loc_params_path * tmp_suffix) : ""
        meta_path, location_path = sync_across((meta_path, location_path), comm=comm)

        # Read in meta path if it's there
        curr_localpaths, curr_nrows = if nbatches > 1 && batch_idx > 1
            let curr_meta = Arrow_Table_retry(meta_path)
                (convert(Base.Vector{String}, curr_meta[:path]), convert(Base.Vector{Int64}, curr_meta[:nrows]))
            end
        else
            (String[], Int64[])
        end

        # Read in the current location if it's there
        empty_df = DataFrames.DataFrame()
        curr_location::Location = if nbatches > 1 && batch_idx > 1
            Banyan.deserialize_retry(location_path)
        else
            LocationSource(
                "Remote",
                Dict(
                    "format" => format_string,
                    "nrows" => 0,
                    "path" => loc_params_path,
                    "meta_path" => meta_path,
                    "empty_sample" => to_jl_value_contents(empty_df)
                ),
                0,
                ExactSample(empty_df, 0)
            )
        end

        # Gather # of rows, # of bytes, empty sample, and actual sample
        nbytes = part_res isa Empty ? 0 : Banyan.total_memory_usage(part_res)
        sample_rate = get_session().sample_rate
        sampled_part = (part_res isa Empty || is_disk) ? empty_df : Banyan.get_sample_from_data(part_res, sample_rate, nrows)
        gathered_data =
            gather_across((nrows, nbytes, part_res isa Empty ? part_res : empty(part_res), sampled_part), comm)
        
        # On the main worker, finalize metadata and location info.
        if is_main
            # Determine paths and #s of rows for metadata file
            for worker_i in 1:nworkers
                push!(
                    curr_localpaths,
                    let sortableidx = Banyan.sortablestring(
                        Banyan.get_partition_idx(batch_idx, nbatches, worker_i),
                        npartitions
                    )
                        joinpath(actualpath, "part_$sortableidx" * ".$format_string")
                    end
                )
            end

            # Update the # of bytes
            total_nrows::Int64 = curr_location.src_parameters["nrows"]
            empty_sample_found = false
            for (new_nrows::Int64, new_nbytes::Int64, empty_part, sampled_part) in gathered_data
                # Update the total # of rows and the total # of bytes
                total_nrows += sum(new_nrows)
                push!(curr_nrows, new_nrows)
                curr_location.total_memory_usage += new_nbytes

                # Get the empty sample
                if !empty_sample_found && !(empty_part isa Empty)
                    curr_location.src_parameters["empty_sample"] = to_jl_value_contents(empty_part)
                    empty_sample_found = true
                end
            end
            curr_location.src_parameters["nrows"] = total_nrows

            # Get the actual sample by concatenating
            curr_location.sample = if is_disk
                Sample()
            else
                sampled_parts = [gathered[4] for gathered in gathered_data]
                if batch_idx > 1
                    push!(sampled_parts, curr_location.sample.value |> seekstart |> Arrow.Table |> DataFrames.DataFrame)
                end
                new_sample_value_arrow = IOBuffer()
                Arrow.write(new_sample_value_arrow, vcat(sampled_parts...), compress=:zstd)
                Sample(new_sample_value_arrow, curr_location.total_memory_usage)
            end

            # Determine paths for this batch and gather # of rows
            Arrow.write(meta_path, (path=curr_localpaths, nrows=curr_nrows), compress=:zstd)

            if !is_disk && batch_idx == nbatches && total_nrows <= get_max_exact_sample_length()
                # If the total # of rows turns out to be inexact then we can simply mark it as
                # stale so that it can be collected more efficiently later on
                # We should be able to quickly recompute a more useful sample later
                # on when we need to use this location.
                curr_location.sample_invalid = true
            end

            # Write out the updated `Location`
            serialize(location_path, curr_location)
        end

        ###################################
        # Handling Final Batch by Copying #
        ###################################

        if nbatches > 1 && batch_idx == nbatches
            # Copy over location and meta path
            actual_meta_path = get_meta_path(loc_params_path)
            actual_location_path = get_location_path(loc_params_path)
            if worker_idx == 1
                cp(meta_path, actual_meta_path, force=true)
                cp(location_path, actual_location_path, force=true)
            end

            # Copy over files to actual location
            tmpdir = readdir(path)
            if is_main
                Banyan.rmdir_on_nfs(actualpath)
                mkpath(actualpath)
            end
            MPI.Barrier(comm)
            for batch_i = 1:nbatches
                idx = Banyan.get_partition_idx(batch_i, nbatches, worker_idx)
                sortableidx = Banyan.sortablestring(idx, get_npartitions(nbatches, comm))
                tmpdir_idx = -1
                for i = 1:length(tmpdir)
                    if contains(tmpdir[i], "part_$sortableidx")
                        tmpdir_idx = i
                        break
                    end
                end
                if tmpdir_idx != -1
                    tmpsrc = joinpath(path, tmpdir[tmpdir_idx])
                    actualdst = joinpath(actualpath, tmpdir[tmpdir_idx])
                    cp(tmpsrc, actualdst, force=true)
                end
            end
            MPI.Barrier(comm)
            if is_main
                Banyan.rmdir_on_nfs(path)
            end
        else
            MPI.Barrier(comm)
        end
        # src
        nothing
        # TODO: Delete all other part* files for this value if others exist
    end
    Write
end

function Banyan.SplitBlock(
    src::DataFrames.AbstractDataFrame,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    Banyan.split_on_executor(
        src,
        1,
        batch_idx,
        nbatches,
        comm,
    )
end

global gdf_cache = IdDict{Any,DataFrames.GroupedDataFrame}()

symbol_divisions_by_partition = "divisions_by_partition"

function SplitGroupDataFrame(
    src::DataFrames.AbstractDataFrame,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    store_splitting_divisions::Bool,
    src_divisions::Base.Vector{Division{V}},
    boundedlower::Bool,
    boundedupper::Bool,
    key::String,
    rev::Bool,
    consolidate::Bool,
    splitting_divisions::IdDict{Any,Any}
) where {V}

    partition_idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
    npartitions = get_npartitions(nbatches, comm)

    # Get divisions_by_partition and partition_idx_getter if needed
    if consolidate || npartitions == 1 || batch_idx == 1 || store_splitting_divisions || true
        @show haskey(params, symbol_divisions_by_partition)
        println("Calling get_divisions in SplitGroup with npartitions=$npartitions")
        divisions_by_partition = if haskey(params, symbol_divisions_by_partition)
            params[symbol_divisions_by_partition]
        else
            Banyan.get_divisions(src_divisions, npartitions)
        end

        # Get the divisions to apply
        if rev
            reverse!(divisions_by_partition)
        end

        partition_idx_getter(val) = Banyan.get_partition_idx_from_divisions(
            val,
            divisions_by_partition,
            boundedlower,
            boundedupper,
        )
    end

    println("In SplitGroupDataFrame on get_worker_idx()=$(get_worker_idx())")


    # Return using a single filter operation if possible
    if consolidate || npartitions == 1 || true
        # Apply divisions to get only the elements relevant to this worker
        filter_mask = Base.falses(nrow(src))
        for (i, row) in enumerate(eachrow(src))
            p_idx = partition_idx_getter(row[key])
            if consolidate
                filter_mask[i] = p_idx !=  -1
            else
                filter_mask[i] = p_idx == partition_idx
            end
        end
        res = src[filter_mask, :]

        record_time(:SplitGroupDataFrame_res_nrow, DataFrames.nrow(res))

        return res
    end

    # Otherwise, use a grouped data frame and cache it to reuse it across batches

    global gdf_cache

    # Compute the grouped data frame
    gdf = if batch_idx == 1
        # Compute the partition to send each row of the dataframe to
        DataFrames.transform!(src, key => ByRow(partition_idx_getter) => :banyan_shuffling_key)

        @show propertynames(src)

        # Group the dataframe's rows by what partition to send to
        gdf_res = DataFrames.groupby(src, :banyan_shuffling_key, sort = true)

        @show combine(gdf_res, nrow)

        gdf_res
    else
        gdf_cache[src]
    end

    gdf_key = (banyan_shuffling_key = partition_idx,)
    @show gdf.cols
    @show gdf.ngroups
    res = if gdf.ngroups > 0 && haskey(gdf, gdf_key)
        gdf_part = gdf[gdf_key]
        @show propertynames(gdf_part)
        gdf_part
    else
        empty(src)
    end

    # Store the grouped data frame
    if nbatches > 1
        if batch_idx == 1
            gdf_cache[src] = gdf
        elseif batch_idx == nbatches
            delete!(gdf_cache, src)
            if hasproperty(src, :banyan_shuffling_key)
                DataFrames.select!(src, Not(:banyan_shuffling_key))
            end
        end
    end

    if hasproperty(res, :banyan_shuffling_key)
        res = DataFrames.select(res, Not(:banyan_shuffling_key), copycols=false)
    end

    if store_splitting_divisions
        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any(isnotempty, divisions_by_partition)
        firstdivisionidx = findfirst(isnotempty, divisions_by_partition)
        lastdivisionidx = findlast(isnotempty, divisions_by_partition)

        # Store divisions
        splitting_divisions[res] = (
            divisions_by_partition[partition_idx],
            !hasdivision || boundedlower || partition_idx != firstdivisionidx,
            !hasdivision || boundedupper || partition_idx != lastdivisionidx,
        )
    end

    res
end

symbol_key = "key"
symbol_rev = "rev"
symbol_consolidate = "consolidate"

function Banyan.SplitGroup(
    src::DataFrames.AbstractDataFrame,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any};
    store_splitting_divisions::Bool = false
)
    npartitions = get_npartitions(nbatches, comm)

    if batch_idx == 1
        splitting_divisions = Banyan.get_splitting_divisions()
        src_divisions, boundedlower, boundedupper = get(splitting_divisions, src) do
            # This case lets us use `SplitGroup` in `DistributeAndShuffle`
            (params["divisions"], get(params, "boundedlower", false), get(params, "boundedupper", false))
        end
        divisions_by_partition = if haskey(params, symbol_divisions_by_partition)
            params[symbol_divisions_by_partition]
        else
            Banyan.get_divisions(src_divisions, npartitions)
        end
        println("In SplitGroup on get_worker_idx()=$(get_worker_idx()) batch_idx=1 with src_divisions=$src_divisions and divisions_by_partition=$divisions_by_partition for npartitions=$npartitions with boundedlower=$boundedlower and boundedupper=$boundedupper and having haskey(splitting_divisions, src)=$(haskey(splitting_divisions, src)) and haskey(params, symbol_divisions_by_partition)=$(haskey(params, symbol_divisions_by_partition)) where minimum(src[:, :sepal_width])=$(minimum(src[:, :sepal_width])) and maximum(src[:, :sepal_width])=$(maximum(src[:, :sepal_width]))")
    end

    # Ensure that this partition has a schema that is suitable for usage
    # here. We have to do this for `Shuffle` and `SplitGroup` (which is
    # used by `DistributeAndShuffle`)
    if isempty(src) || npartitions == 1
        # TODO: Ensure we can return here like this and don't need the above
        # (which is copied from `Shuffle`)
        return src
    end

    splitting_divisions = Banyan.get_splitting_divisions()
    println("In SplitGroup with haskey(splitting_divisions, src)=$(haskey(splitting_divisions, src)) and params=$params and get_worker_idx()=$(get_worker_idx())")
    src_divisions, boundedlower, boundedupper = get(splitting_divisions, src) do
        # This case lets us use `SplitGroup` in `DistributeAndShuffle`
        (params["divisions"], get(params, "boundedlower", false), get(params, "boundedupper", false))
    end
    SplitGroupDataFrame(
        src,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        loc_params,
        store_splitting_divisions,
        src_divisions,
        boundedlower,
        boundedupper,
        params[symbol_key]::String,
        get(params, symbol_rev, false)::Bool,
        # If true, SplitGroup should return all the data that matches any
        # of the given splitting divisions.
        get(params, symbol_consolidate, false)::Bool,
        splitting_divisions
    )
end

# It's only with BanyanDataFrames to we have block-partitioned things that can
# be merged to become nothing.
# Grouped data frames can be block-partitioned but we will have to
# redo the groupby if we try to do any sort of merging/splitting on it.

function RebalanceDataFrame(
    part::DataFrames.DataFrame,
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
    # TODO: Use JLD for ser/de for arrays
    # TODO: Ensure that we are properly handling intermediate arrays or
    # dataframes that are empty (especially because they may not have their
    # ndims or dtype or schema). We probably are because dataframes that are
    # empty should concatenate properly. We just need to be sure to not expect
    # every partition to know what its schema is. We can however expect each
    # partition of an array to know its ndims.

    # Construct buffer to send parts to all workers who own in this range
    nworkers = Banyan.get_nworkers(comm)
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts::Base.Vector{Int64} = Int64[]
    for partition_idx = 1:npartitions
        # `Banyan.split_len` gives us the range that this partition needs
        partitionrange = Banyan.split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap =
            max(startidx, partitionrange.start) <= min(endidx, partitionrange.stop)

        # If they do overlap, then serialize the overlapping slice
        Arrow.write(
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
            compress=:zstd 
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
        things_to_concatenate,
        dim,
    )
    res
end

RebalanceDataFrame(
    part::Empty,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) = RebalanceDataFrame(
    DataFrames.DataFrame(),
    src_params,
    dst_params,
    comm
)

# If this is a grouped data frame or nothing (the result of merging
# a grouped data frame is nothing), we consolidate by simply returning
# nothing.

function ConsolidateDataFrame(part::DataFrames.DataFrame, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm)
    io = IOBuffer()
    Arrow.write(io, part, compress=:zstd)
    sendbuf = MPI.Buffer(view(io.data, 1:io.size))
    recvvbuf = Banyan.buftovbuf(sendbuf, comm)
    # TODO: Maybe sometimes use gatherv if all sendbuf's are known to be equally sized

    println("In ConsolidateDataFrame before MPI.Allgatherv! on get_worker_idx()=$(get_worker_idx())")
    MPI.Allgatherv!(sendbuf, recvvbuf, comm)
    println("In ConsolidateDataFrame after MPI.Allgatherv! on get_worker_idx()=$(get_worker_idx())")
    res = merge_on_executor(
        [
            de(view(
                recvvbuf.data,
                (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
            ))
            for i in 1:Banyan.get_nworkers(comm)
        ],
        1
    )
    res
end

ConsolidateDataFrame(
    part::Empty,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) = ConsolidateDataFrame(
    DataFrames.DataFrame(),
    src_params,
    dst_params,
    comm
)

function combine_in_memory(a, b, groupcols, groupkwargs, combinecols, combineargs, combinekwargs)
    concatenated = vcat(a, b)
    grouped = groupby(concatenated, groupcols; groupkwargs...)
    combined = combine(grouped, combineargs...; combinekwargs...)
end

function ReduceDataFrame(
    part,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
)
    # res = reduce_and_sync_across(src_params["reducing_op"], part; comm=comm)
    println("At start of ReduceDataFrame on get_worker_idx()=$(get_worker_idx())")
    res = ConsolidateDataFrame(part, Dict{String,Any}(), Dict{String,Any}(), comm)
    res = src_params["reducing_op"](res, DataFrames.DataFrame()) 
    res_finished = src_params["finishing_op"](res)
    println("At end of ReduceDataFrame on get_worker_idx()=$(get_worker_idx())")
    res_finished
end

ReduceDataFrame(part::Empty, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) =
    ReduceDataFrame(DataFrames.DataFrame(), src_params, dst_params, comm)

function ReduceAndCopyToArrow(
    src,
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    @nospecialize(start_op::Function),
    @nospecialize(reduce_op::Function),
    @nospecialize(finish_op::Function)
) where {T}
    # Get the # of rows of each group if this is a group-by-mean that is being reduced
    et = @elapsed begin
    if loc_name == "Memory"
        part = start_op(part)
    end

    # Concatenate all data frames on this worker
    src = if nbatches > 1
        Merge(
            src,
            part,
            params,
            batch_idx,
            nbatches,
            MPI.COMM_SELF,
            loc_name,
            loc_params
        )
    else
        part
    end

    # Merge reductions across workers
    if batch_idx == nbatches
        # TODO: Eliminate the redundant concatenation with empty data frames that the reduce_op
        # performs for group-by aggregation
        src = reduce_op(src, DataFrames.DataFrame())

        if get_nworkers(comm) > 1
            src = ConsolidateDataFrame(src, Dict{String,Any}(), Dict{String,Any}(), comm)
            src = reduce_op(src, DataFrames.DataFrame())
            # NOTE: We use the above to eliminate the compilation overhead of creating
            # custom MPI reductions
            # src = reduce_across(reduce_op, src, comm=comm)
        end

        if loc_name != "Memory"
            if is_main_worker(comm)
                src = finish_op(src)
            else
                src = DataFrames.DataFrame()
            end
            CopyToArrow(src, src, params, 1, 1, comm, loc_name, loc_params)
        end
    end
    end
    record_time(:reduction, et)
    
    src
end

function ReduceAndCopyToArrow(
    src,
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    if loc_name == "Remote"
        @time "ReduceAndCopyToArrow" begin
        ReduceAndCopyToArrow(
            src isa Empty ? DataFrames.DataFrame() : src,
            part,
            params,
            batch_idx,
            nbatches,
            comm,
            loc_name,
            loc_params,
            params["starting_op"],
            params["reducing_op"],
            params["finishing_op"]
        )
        end
    else
        ReduceAndCopyToArrow(
            src isa Empty ? DataFrames.DataFrame() : src,
            part,
            params,
            batch_idx,
            nbatches,
            comm,
            loc_name,
            loc_params,
            params["starting_op"],
            params["reducing_op"],
            params["finishing_op"]
        )
    end
end