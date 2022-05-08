get_file_ending(remotepath::String)::String = splitext(remotepath)[2][2:end]

function _remote_table_source(remotepath, shuffled, metadata_invalid, sample_invalid, invalidate_metadata, invalidate_sample)::Location
    session_sample_rate = get_session().sample_rate
    is_main = is_main_worker()
    
    # Get cached Location and if it has valid parameters and sample, return
    @time begin
    et = @elapsed begin
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location(remotepath, metadata_invalid, sample_invalid)
    end
    println("Time for get_cached_location: $et seconds")
    end
    if !curr_parameters_invalid && !curr_sample_invalid
        return curr_location
    end

    # There are two things we cache for each call `to _remote_table_source`:
    # 1. A `Location` serialized to a `location_path`
    # 2. Metadata stored in an Arrow file at `meta_path`

    # Get metadata if it is still valid
    @time begin
    et = @elapsed begin
    curr_meta::Arrow.Table = if !curr_parameters_invalid
        Arrow.Table(curr_location.parameters["meta_path"]::String)
    else
        Arrow.Table()
    end
    end
    println("Time for Arrow.Table: $et seconds")
    end

    # Metadata file structure
    # - Arrow file
    # - Constructed with a `NamedTuple` mapping to `Vector`s and read in as an Arrow.Table
    # - Columns: file name, # of rows, # of bytes

    # Get list of local paths
    @time begin
    et = @elapsed begin
    localpaths::Base.Vector{String} = if !curr_parameters_invalid
        convert(Base.Vector{String}, curr_meta[:path])
    else
        localpath::String = getpath(remotepath)
        localpath_is_dir = isdir(localpath)
        paths = if localpath_is_dir
            paths_on_main = is_main ? readdir(localpath, join=true) : String[]
            sync_across(paths_on_main)
        else
            String[localpath]
        end
        paths
    end
    curr_meta_nrows::Base.Vector{Int64} = !curr_parameters_invalid ? convert(Base.Vector{Int64}, curr_meta[:nrows]) : Int64[]
    local_paths_on_curr_worker::Base.Vector{String} = split_across(localpaths)
    end
    println("Time to sync_across with paths to read in: $et seconds")
    end

    # Get format
    format_string = get_file_ending(remotepath)
    format_value = Val(Symbol(format_string))
    format_has_separate_metadata = has_separate_metadata(format_value)

    # Get nrows, nbytes for each file in local_paths_on_curr_worker
    @time begin
    et = @elapsed begin
    meta_nrows_on_worker = if curr_parameters_invalid
        meta_nrows_on_worker_res = Base.zeros(length(local_paths_on_curr_worker))
        if format_has_separate_metadata
            for (i, local_path_on_curr_worker) in enumerate(local_paths_on_curr_worker)
                path_nrows_on_worker = get_metadata(format_value, local_path_on_curr_worker)
                meta_nrows_on_worker_res[i] = path_nrows_on_worker
            end
        end
        # If this format doesn't have separate metadata, we will have to
        # read it in later along with the sample itself.
        meta_nrows_on_worker_res
    else
        split_across(curr_meta_nrows)
    end
    end
    println("Time for getting metadata: $et seconds")
    end

    # Compute the total # of rows so that if the current sample is invalid
    # we can determine whether to get an exact or inexact sample and
    # otherwise so that we can update the sample rate.
    total_nrows_res = if curr_parameters_invalid
        if format_has_separate_metadata
            @time begin
            et = @elapsed begin
            reduce_and_sync_across_res = reduce_and_sync_across(+, sum(meta_nrows_on_worker))
            end
            println("Time for reduce_and_sync_across: $et seconds")
            end
            reduce_and_sync_across_res
        else
            # For formats with metadata stored with the data (CSV), we
            # determine the # of rows later in the below case where
            # `!is_metadata_valid``.
            -1
        end
    else
        curr_location.parameters["nrows"]
    end
    exact_sample_needed = total_nrows_res < Banyan.get_max_exact_sample_length()

    # inv: (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
    # (c) `exact_sample_needed` are only valid if either the format has
    # separate metadata (like Parquet and Arrow) or the metadata is already
    # stored and valid.
    is_metadata_valid = format_has_separate_metadata || !curr_parameters_invalid
    # If the metadata isn't valid then we anyway have to read in all the data
    # so we can't leverage the data being shuffled by only reading in some of the files
    shuffled = shuffled && is_metadata_valid && !exact_sample_needed
    # TODO: Use this

    # Get sample and also metadata if not yet valid at this point
    recollected_sample_needed = curr_sample_invalid || !is_metadata_valid
    meta_nrows, total_nrows, total_nbytes, remote_sample::Sample, empty_sample::String = if recollected_sample_needed
        # In this case, we actually recollect a sample. This is the case
        # where either we actually have an invalid sample or the sample is
        # valid but the metadata is changed and the format is such that
        # recollecting metadata information would be more expensive than
        # any recollection of sample.

        # Get local sample
        @time begin
        et = @elapsed begin
        local_samples = DataFrames.DataFrame[]
        if is_metadata_valid
            # Determine which files to read from if shuffled
            shuffling_perm, nfiles_on_worker, nrows_extra_on_worker = if shuffled
                perm_for_shuffling = randperm(length(meta_nrows_on_worker))
                shuffled_meta_nrows_on_worker = meta_nrows_on_worker[perm_for_shuffling]
                @show shuffled_meta_nrows_on_worker
                nrows_on_worker_so_far = 0
                nrows_on_worker_target = cld(sum(meta_nrows_on_worker), session_sample_rate)
                @show nrows_on_worker_target
                nfiles_on_worker_res = 0
                for nrows_on_worker in shuffled_meta_nrows_on_worker
                    nrows_on_worker_so_far += nrows_on_worker
                    nfiles_on_worker_res += 1
                    if nrows_on_worker_so_far >= nrows_on_worker_target
                        break
                    end
                end
                shuffling_perm, nfiles_on_worker_res, nrows_on_worker_so_far - nrows_on_worker_target
            else
                Colon(), length(local_paths_on_curr_worker), 0
            end
            @show shuffling_perm
            @show nfiles_on_worker
            @show nrows_extra_on_worker
            meta_nrows_for_worker = meta_nrows_on_worker[shuffling_perm]

            # Get local sample
            for (i, local_path_on_curr_worker) in Base.collect(zip(local_paths_on_curr_worker[shuffling_perm], 1:nfiles_on_worker))
                @time begin
                et = @elapsed begin
                push!(
                    local_samples,
                    let df = get_sample(
                        format_value,
                        local_path_on_curr_worker,
                        (shuffled || exact_sample_needed) ? 1.0 : session_sample_rate,
                        meta_nrows_for_worker[i]
                    )
                        if shuffled && i == nfiles_on_worker && nrows_extra_on_worker > 0
                            df[1:(end-nrows_extra_on_worker), :]
                        else
                            df
                        end
                    end
                )
                end
                println("Time to call get_sample: $et seconds")
                end
            end
        else
            # This is the case for formats like CSV where we must read in the
            # metadata with the data AND the metadata is stale and couldn't
            # just have been read from the Arrow metadata file.

            local_nrows = 0
            # First see if we can get a random (inexact sample).
            for exact_sample_needed_res in [false, true]
                empty!(local_samples)
                local_nrows = 0
                for (i, local_path_on_curr_worker) in enumerate(local_paths_on_curr_worker)
                    @time begin
                    et = @elapsed begin
                    path_sample, path_nrows = get_sample_and_metadata(
                        format_value,
                        local_path_on_curr_worker,
                        exact_sample_needed ? 1.0 : session_sample_rate
                    )
                    meta_nrows_on_worker[i] = path_nrows
                    push!(local_samples, path_sample)
                    local_nrows += path_nrows
                    end
                    println("Time on worker_idx=$(get_worker_idx()) to call get_sample_and_metadata: $et seconds")
                    end
                end
                total_nrows_res = reduce_and_sync_across(+, local_nrows)
                # If the sample is too small, redo it, getting an exact sample
                if !exact_sample_needed_res && total_nrows_res < Banyan.get_max_exact_sample_length()
                    exact_sample_needed = true
                    exact_sample_needed_res = true
                else
                    exact_sample_needed = false
                    break
                end
            end
        end
        @time begin
        et = @elapsed begin
        local_sample::DataFrames.DataFrame = isempty(local_samples) ? DataFrames.DataFrame() : vcat(local_samples...)
        end
        println("Time to vcat local_samples: $et seconds")
        end
        end
        println("Time to get samples locally: $et seconds")
        end

        # Concatenate local samples and nrows together
        @time begin
        et = @elapsed begin
        remote_sample_value::DataFrames.DataFrame, meta_nrows_on_workers::Base.Vector{Int64} = if curr_parameters_invalid
            sample_and_meta_nrows_per_worker::Base.Vector{Tuple{DataFrames.DataFrame,Base.Vector{Int64}}} =
                gather_across((local_sample, meta_nrows_on_worker))
            if is_main
                sample_per_worker = DataFrames.DataFrame[]
                meta_nrows_per_worker = Int64[]
                for sample_and_meta_nrows in sample_and_meta_nrows_per_worker
                    push!(sample_per_worker, sample_and_meta_nrows[1])
                    push!(meta_nrows_per_worker, sample_and_meta_nrows[2])
                end
                @time begin
                et = @elapsed begin
                res = vcat(sample_per_worker...), vcat(meta_nrows_per_worker...)
                end
                println("Time on worker_idx=$(get_worker_idx()) to vcat sample_per_worker and meta_nrows_per_Worker: $et seconds")
                end
                res
            else
                DataFrames.DataFrame(), Int64[]
            end
        else
            sample_per_worker = gather_across(local_sample)
            @show sample_per_worker
            @show get_worker_idx()
            error("hello")
            if is_main && !isempty(sample_per_worker)
                vcat(sample_per_worker...), curr_meta_nrows
            else
                DataFrames.DataFrame(), Int64[]
            end
        end
        end
        println("Time on worker_idx=$(get_worker_idx()) to concatenate samples across workers: $et seconds")
        end

        # At this point the metadata is valid regardless of whether this
        # format has metadata stored separately or not. We have a valid
        # (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
        # (c) `exact_sample_needed`.
        is_metadata_valid = true

        # Return final Sample on main worker now that we have gathered both the sample and metadata
        if is_main
            @time begin
            et = @elapsed begin
            empty_sample_value_serialized::String = to_jl_value_contents(empty(remote_sample_value))
            end
            println("Time on worker_idx=$(get_worker_idx()) to call to_jl_value_contents on an empty data frame: $et seconds")
            end

            # Construct Sample with the concatenated value, memory usage, and sample rate
            remote_sample_value_memory_usage = total_memory_usage(remote_sample_value)
            total_nbytes_res = if exact_sample_needed
                remote_sample_value_memory_usage
            else
                ceil(Int64, remote_sample_value_memory_usage * session_sample_rate)
            end
            remote_sample_res::Sample = if exact_sample_needed
                # Technically we don't need to be passing in `total_bytes_res`
                # here but we do it because we are anyway computing it to
                # return as the `total_memory_usage` for the `Location` and so
                # we might as well avoid recomputing it in the `Sample`
                # constructors
                ExactSample(remote_sample_value, total_nbytes_res)
            else
                Sample(remote_sample_value, total_nbytes_res)
            end
            # TODO: Ensure all if statements are handled
            # TODO: Ensure that the right stuff is running on main worker
            meta_nrows_on_workers, total_nrows_res, total_nbytes_res, remote_sample_res, empty_sample_value_serialized
        else
            Int64[], -1, -1, NOTHING_SAMPLE, to_jl_value_contents(DataFrames.DataFrame())
        end
    else
        # This case is entered if we the format has metadata stored
        # separately and we only had to recollect the metadata and could
        # avoid recollecting the sample as we would in the other case.

        # inv: is_metadata_valid == true

        # If the sample is valid, the metadata must be invalid and need concatenation.
        meta_nrows_per_worker::Base.Vector{Base.Vector{Int64}} = gather_across(meta_nrows_on_worker)
        if is_main
            meta_nrows_res::Base.Vector{Int64} = vcat(meta_nrows_per_worker...)

            # Get the total # of bytes
            cached_remote_sample_res::Sample = curr_location.sample
            remote_sample_value_nrows = nrow(cached_remote_sample_res.value)
            remote_sample_value_nbytes = total_memory_usage(cached_remote_sample_res.value)
            total_nbytes_res = ceil(Int64, remote_sample_value_nbytes * total_nrows_res / remote_sample_value_nrows)

            # Update the sample's sample rate and memory usage based on the
            # new # of rows (since the metadata with info about # of rows
            # has been invalidated)
            cached_remote_sample_res.rate = ceil(Int64, total_nrows_res / remote_sample_value_nrows)
            cached_remote_sample_res.memory_usage = ceil(Int64, total_nbytes_res / cached_remote_sample_res.rate)::Int64

            meta_nrows_res, total_nrows_res, total_nbytes_res, cached_remote_sample_res, curr_location.src_parameters["empty_sample"]
        else
            Int64[], -1, -1, NOTHING_SAMPLE, to_jl_value_contents(DataFrames.DataFrame())
        end
    end

    # If a file does not exist, one of the get_metadata/get_sample functions
    # will error.

    # Write the metadata to an Arrow file
    if is_main && curr_parameters_invalid
        @time begin
        et = @elapsed begin
        meta_path = get_meta_path(remotepath)
        end
        println("Time to get_meta_path: $et seconds")
        end
        
        # Write `NamedTuple` with metadata to `meta_path` with `Arrow.write`
        @time begin
        et = @elapsed begin
        Arrow.write(meta_path, (path=localpaths, nrows=meta_nrows))
        end
        println("Time to Arrow.write metadata: $et seconds")
        end
    end

    # TODO: Modify invalidate_* functions
    # TODO: Modify location destination constructor
    # TODO: Modify other `locations.jl` files in BanyanHDF5.jl, BanyanImages.jl
    # TODO: Make writing PFs update sample and parameters and make write_*
    # functions not invalidate the sample

    # Return LocationSource
    if is_main
        # Construct the `Location` to return
        location_res = LocationSource(
            "Remote",
            Dict(
                # For dispatching the appropriate PF for this format
                "format" => format_string,
                # For constructing the `BanyanDataFrames.DataFrame`'s `nrows::Future` field
                "nrows" => total_nrows,
                # For diagnostics purposes in PFs (partitioning functions)
                "path" => remotepath,
                # For PFs to read from this source
                "meta_path" => meta_path,
                "empty_sample" => empty_sample
            ),
            total_nbytes,
            remote_sample
        )

        # Write out the updated `Location`
        @time begin
        et = @elapsed begin
        cache_location(remotepath, location_res, invalidate_sample, invalidate_metadata)
        end
        println("Time to cache_location: $et seconds")
        end

        location_res
    else
        NOTHING_LOCATION
    end
end

function RemoteTableSource(remotepath; shuffled=true, metadata_invalid = false, sample_invalid = false, invalidate_metadata = false, invalidate_sample = false)::Location
    offloaded(
        _remote_table_source,
        remotepath,
        shuffled,
        metadata_invalid,
        sample_invalid,
        invalidate_metadata,
        invalidate_sample;
        distributed=true
    )
end

# function RemoteTableSource(remotepath; shuffled=false, metadata_invalid = false, sample_invalid = false, invalidate_metadata = false, invalidate_sample = false)::Location
#     if Banyan.INVESTIGATING_CACHING_LOCATION_INFO || Banyan.INVESTIGATING_CACHING_SAMPLES
#         println("Before call to RemoteSource")
#         @show remotepath metadata_invalid sample_invalid
#     end
#     RemoteSource(
#         get_remote_table_source,
#         remotepath,
#         shuffled,
#         metadata_invalid,
#         sample_invalid,
#         invalidate_metadata,
#         invalidate_sample
#     )
# end

# Load metadata for writing
# NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
# or CSV dataset is desired to be created
RemoteTableDestination(remotepath)::Location =
    LocationDestination(
        "Remote",
        Dict(
            "format" => get_file_ending(remotepath),
            "nrows" => 0,
            "path" => remotepath,
        ),
    )