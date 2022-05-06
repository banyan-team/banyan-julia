get_file_ending(remotepath::String)::String = splitext(remotepath)[2][2:end]

function _remote_table_source(remotepath, shuffled, source_invalid, sample_invalid, invalidate_source, invalidate_sample)::Location
    session_sample_rate = get_session().sample_rate
    is_main = is_main_worker()
    
    # Get cached Location and if it has valid parameters and sample, return
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location(remotepath, source_invalid, sample_invalid)
    if !curr_parameters_invalid && !curr_sample_invalid
        return curr_location
    end

    # There are two things we cache for each call `to _remote_table_source`:
    # 1. A `Location` serialized to a `location_path`
    # 2. Metadata stored in an Arrow file at `meta_path`

    # Get metadata if it is still valid
    curr_meta::Arrow.Table = if is_main && !curr_parameters_invalid
        Arrow.Table(curr_location.parameters["meta_path"]::String)
    else
        Arrow.Table()
    end

    # Metadata file structure
    # - Arrow file
    # - Constructed with a `NamedTuple` mapping to `Vector`s and read in as an Arrow.Table
    # - Columns: file name, # of rows, # of bytes

    # Get list of local paths
    localpaths_on_main_worker::Base.Vector{String}, localpaths_shuffling_on_main::Base.Vector{Int64} = if is_main
        paths = if !curr_parameters_invalid
            convert(Base.Vector{String}, curr_meta[:path])
        else
            localpath::String = getpath(remotepath)
            localpath_is_dir = isdir(localpath)
            paths = localpath_is_dir ? readdir(localpath, join=true) : String[localpath]
            paths
        end
        paths, shuffled ? randsubseq(1:length(paths), 1 / session_sample_rate) : Int64[]
    else
        String[], Int64[]
    end
    localpaths::Base.Vector{String}, localpaths_shuffling::Base.Vector{String}, curr_meta_nrows::Base.Vector{Int64} =
        sync_across(
            (
                localpaths_on_main_worker,
                localpaths_shuffling_on_main,
                (!curr_parameters_invalid && is_main) ? convert(Base.Vector{Int64}, curr_meta[:nrows]) : Int64[]
            )
        )
    local_paths_on_curr_worker::Base.Vector{String} = split_across(localpaths)
    local_paths_shuffling_on_curr_worker::Base.Vector{String} = shuffled ? split_across(localpaths_shuffling) : String[]

    # Get format
    format_string = get_file_ending(remotepath)
    format_value = Val(Symbol(format_string))

    # Get nrows, nbytes for each file in local_paths_on_curr_worker
    meta_nrows_on_worker = if curr_parameters_invalid
        meta_nrows_on_worker_res = Base.zeros(length(local_paths_on_curr_worker))
        if has_separate_metadata(format_value)
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

    # Compute the total # of rows so that if the current sample is invalid
    # we can determine whether to get an exact or inexact sample and
    # otherwise so that we can update the sample rate.
    total_nrows_res = if curr_parameters_invalid
        if has_separate_metadata(format_value)
            reduce_and_sync_across(+, sum(meta_nrows_on_worker))
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
    is_metadata_valid = has_separate_metadata(format_value) || !curr_parameters_invalid

    # Get sample and also metadata if not yet valid at this point
    recollected_sample_needed = curr_sample_invalid || !is_metadata_valid
    meta_nrows, total_nrows, total_nbytes, remote_sample::Sample, empty_sample::String = if recollected_sample_needed
        # In this case, we actually recollect a sample. This is the case
        # where either we actually have an invalid sample or the sample is
        # valid but the metadata is changed and the format is such that
        # recollecting metadata information would be more expensive than
        # any recollection of sample.

        # Get local sample
        local_samples = DataFrames.DataFrame[]
        if is_metadata_valid
            # Get local sample
            paths_to_sample_from =
                if shuffled
                    zip(1:length(paths_to_sample_from), paths_to_sample_from)
                else
                    zip(local_paths_shuffling_on_curr_worker, localpaths[local_paths_shuffling_on_curr_worker])
                end
            for (i, local_path_on_curr_worker) in paths_to_sample_from
                push!(
                    local_samples,
                    get_sample(
                        format_value,
                        local_path_on_curr_worker,
                        exact_sample_needed ? 1.0 : session_sample_rate,
                        meta_nrows_on_worker[i]
                    )
                )
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
                    path_sample, path_nrows = get_sample_and_metadata(
                        format_value,
                        local_path_on_curr_worker,
                        exact_sample_needed ? 1.0 : session_sample_rate
                    )
                    meta_nrows_on_worker[i] = path_nrows
                    push!(local_samples, path_sample)
                    local_nrows += path_nrows
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
        local_sample::DataFrames.DataFrame = isempty(local_samples) ? DataFrames.DataFrame() : vcat(local_samples...)

        # Concatenate local samples and nrows together
        remote_sample_value::DataFrames.DataFrame, meta_nrows_on_workers::Base.Vector{Int64} = if curr_parameters_invalid
            sample_and_meta_nrows_per_worker::Base.Vector{Tuple{DataFrames.DataFrame,Base.Vector{Int64}}} =
                gather_across((local_sample, meta_nrows_on_worker))
            if is_main
                vcat(sample_and_meta_nrows_per_worker[1]...), vcat(sample_and_meta_nrows_per_worker[2]...)
            else
                DataFrames.DataFrame(), Int64[]
            end
        else
            sample_per_worker = gather_across(local_sample)
            @show sample_per_worker
            @show get_worker_idx()
            if is_main && !isempty(sample_per_worker)
                vcat(sample_per_worker...), curr_meta_nrows
            else
                DataFrames.DataFrame(), Int64[]
            end
        end

        # At this point the metadata is valid regardless of whether this
        # format has metadata stored separately or not. We have a valid
        # (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
        # (c) `exact_sample_needed`.
        is_metadata_valid = true

        # Return final Sample on main worker now that we have gathered both the sample and metadata
        if is_main
            empty_sample_value_serialized::String = to_jl_value_contents(empty(remote_sample_value))

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
        meta_path = get_meta_path(remotepath)
        
        # Write `NamedTuple` with metadata to `meta_path` with `Arrow.write`
        Arrow.write(meta_path, (path=localpaths, nrows=meta_nrows))
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
        cache_location(remotepath, location_res, invalidate_sample, invalidate_source)

        location_res
    else
        NOTHING_LOCATION
    end
end

function RemoteTableSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    offloaded(
        _remote_table_source,
        remotepath,
        shuffled,
        source_invalid,
        sample_invalid,
        invalidate_source,
        invalidate_sample;
        distributed=true
    )
end

# function RemoteTableSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
#     if Banyan.INVESTIGATING_CACHING_LOCATION_INFO || Banyan.INVESTIGATING_CACHING_SAMPLES
#         println("Before call to RemoteSource")
#         @show remotepath source_invalid sample_invalid
#     end
#     RemoteSource(
#         get_remote_table_source,
#         remotepath,
#         shuffled,
#         source_invalid,
#         sample_invalid,
#         invalidate_source,
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