get_file_ending(remotepath::String)::String = splitext(remotepath)[2][2:end]

Arrow_Table_retry = retry(Arrow.Table; delays=Base.ExponentialBackOff(; n=5))

function _remote_table_source(remotepath, shuffled, metadata_invalid, sample_invalid, invalidate_metadata, invalidate_sample, max_exact_sample_length)::Location
    session_sample_rate = get_session().sample_rate
    is_main = is_main_worker()
    
    # Get cached Location and if it has valid parameters and sample, return
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location(remotepath, metadata_invalid, sample_invalid)
    if !curr_parameters_invalid && !curr_sample_invalid
        return curr_location
    end

    # There are two things we cache for each call `to _remote_table_source`:
    # 1. A `Location` serialized to a `location_path`
    # 2. Metadata stored in an Arrow file at `meta_path`

    # Get metadata if it is still valid
    curr_meta::Arrow.Table = if !curr_parameters_invalid
        Arrow_Table_retry(curr_location.src_parameters["meta_path"]::String)
    else
        Arrow.Table()
    end

    # Metadata file structure
    # - Arrow file
    # - Constructed with a `NamedTuple` mapping to `Vector`s and read in as an Arrow.Table
    # - Columns: file name, # of rows, # of bytes

    # Get list of local paths. Note that in the future when we support a list of
    # Internet locations, we will want to only call getpath laterin this code when/if
    # we actually read stuff in.
    localpaths::Base.Vector{String}, remotepaths::Base.Vector{String} = if !curr_parameters_invalid
        remotepaths_res = convert(Base.Vector{String}, curr_meta[:path])
        map(getpath, remotepaths_res), remotepaths_res
    else
        localpath::String = getpath(remotepath)
        localpath_is_dir = isdir(localpath)
        if localpath_is_dir
            paths_on_main = is_main ? readdir(localpath, join=false) : String[]
            paths = sync_across(paths_on_main)
            npaths = length(paths)
            localpaths_res = Base.Vector{String}(undef, npaths)
            remotepaths_res = Base.Vector{String}(undef, npaths)
            for i = 1:npaths
                localpaths_res[i] = joinpath(localpath, paths[i])
                remotepaths_res[i] = joinpath(remotepath, paths[i])
            end
            localpaths_res, remotepaths_res
        else
            String[localpath], String[remotepath]
        end
    end
    curr_meta_nrows::Base.Vector{Int64} = !curr_parameters_invalid ? convert(Base.Vector{Int64}, curr_meta[:nrows]) : Int64[]
    local_paths_on_curr_worker::Base.Vector{String} = split_across(localpaths)

    # Get format
    format_string = get_file_ending(remotepath)
    format_value = Val(Symbol(format_string))
    format_has_separate_metadata = has_separate_metadata(format_value)

    # Get nrows, nbytes for each file in local_paths_on_curr_worker
    meta_nrows_on_worker::Base.Vector{Int64} = if curr_parameters_invalid
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

    if Banyan.INVESTIGATING_COLLECTING_SAMPLES
        println("In _remote_table_source on get_worker_idx()=$(get_worker_idx()) with curr_sample_invalid=$curr_sample_invalid, curr_parameters_invalid=$curr_parameters_invalid, localpaths=$localpaths, remotepaths=$remotepaths, local_paths_on_curr_worker=$local_paths_on_curr_worker, meta_nrows_on_worker=$meta_nrows_on_worker")
    end

    # Compute the total # of rows so that if the current sample is invalid
    # we can determine whether to get an exact or inexact sample and
    # otherwise so that we can update the sample rate.
    total_nrows_res = if curr_parameters_invalid
        if format_has_separate_metadata
            reduce_and_sync_across(+, sum(meta_nrows_on_worker))
        else
            # For formats with metadata stored with the data (CSV), we
            # determine the # of rows later in the below case where
            # `!is_metadata_valid``.
            -1
        end
    else
        curr_location.src_parameters["nrows"]
    end
    exact_sample_needed = total_nrows_res < max_exact_sample_length

    # inv: (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
    # (c) `exact_sample_needed` are only valid if either the format has
    # separate metadata (like Parquet and Arrow) or the metadata is already
    # stored and valid.
    is_metadata_valid = format_has_separate_metadata || !curr_parameters_invalid
    # If the metadata isn't valid then we anyway have to read in all the data
    # so we can't leverage the data being shuffled by only reading in some of the files
    shuffled = shuffled && is_metadata_valid && !exact_sample_needed

    # Get sample and also metadata if not yet valid at this point
    recollected_sample_needed = curr_sample_invalid || !is_metadata_valid
    if Banyan.INVESTIGATING_COLLECTING_SAMPLES
        println("In _remote_table_source on get_worker_idx()=$(get_worker_idx()) with is_metadata_valid=$is_metadata_valid, shuffled = $shuffled, recollected_sample_needed=$recollected_sample_needed")
    end
    meta_nrows, total_nrows, total_nbytes, remote_sample::Sample, empty_sample::String = if recollected_sample_needed
        # In this case, we actually recollect a sample. This is the case
        # where either we actually have an invalid sample or the sample is
        # valid but the metadata is changed and the format is such that
        # recollecting metadata information would be more expensive than
        # any recollection of sample.

        # Get local sample
        local_samples = DataFrames.DataFrame[]
        if is_metadata_valid
            # Determine which files to read from if shuffled
            shuffling_perm, nfiles_on_worker, nrows_extra_on_worker = if shuffled
                perm_for_shuffling = randperm(length(meta_nrows_on_worker))
                shuffled_meta_nrows_on_worker = meta_nrows_on_worker[perm_for_shuffling]
                nrows_on_worker_so_far = 0
                nrows_on_worker_target = cld(sum(meta_nrows_on_worker), session_sample_rate)
                nfiles_on_worker_res = 0
                for nrows_on_worker in shuffled_meta_nrows_on_worker
                    nrows_on_worker_so_far += nrows_on_worker
                    nfiles_on_worker_res += 1
                    if nrows_on_worker_so_far >= nrows_on_worker_target
                        break
                    end
                end
                if Banyan.INVESTIGATING_COLLECTING_SAMPLES
                    println("In _remote_table_source on get_worker_idx()=$(get_worker_idx()) with nrows_on_worker_target=$nrows_on_worker_target, nfiles_on_worker_res=$nfiles_on_worker_res, nrows_on_worker_so_far=$nrows_on_worker_so_far")
                end
                perm_for_shuffling, nfiles_on_worker_res, nrows_on_worker_so_far - nrows_on_worker_target
            else
                Colon(), length(local_paths_on_curr_worker), 0
            end
            meta_nrows_for_worker = meta_nrows_on_worker[shuffling_perm]

            # Get local sample
            for (i, local_path_on_curr_worker) in zip(1:nfiles_on_worker, local_paths_on_curr_worker[shuffling_perm])
                push!(
                    local_samples,
                    let df = get_sample(
                        format_value,
                        local_path_on_curr_worker,
                        (shuffled || exact_sample_needed) ? 1.0 : session_sample_rate,
                        meta_nrows_for_worker[i]::Int64
                    )
                        if Banyan.INVESTIGATING_COLLECTING_SAMPLES
                            println("Sampling on get_worker_idx()=$(get_worker_idx()) from local_path_on_curr_worker=$local_path_on_curr_worker with session_sample_rate=$session_sample_rate with meta_nrows_for_worker[i]=$(meta_nrows_for_worker[i]) and i=$i with nrow(df)=$(DataFrames.nrow(df)) and nrows_extra_on_worker=$nrows_extra_on_worker")
                        end
                        if shuffled && i == nfiles_on_worker && nrows_extra_on_worker > 0
                            df[1:(end-nrows_extra_on_worker), :]
                        else
                            df
                        end
                    end
                )
            end

            if Banyan.INVESTIGATING_COLLECTING_SAMPLES
                println("In _remote_table_source on get_worker_idx()=$(get_worker_idx()) with shuffling_perm=$shuffling_perm, nfiles_on_worker=$nfiles_on_worker, nrows_extra_on_worker=$nrows_extra_on_worker")
            end
        else
            # This is the case for formats like CSV where we must read in the
            # metadata with the data AND the metadata is stale and couldn't
            # just have been read from the Arrow metadata file.

            local_nrows = 0
            for exact_sample_needed_res in [false, true]
                # First see if we can get a random (inexact sample).
                empty!(local_samples)
                local_nrows = 0
                for (i, local_path_on_curr_worker) in enumerate(local_paths_on_curr_worker)
                    path_sample, path_nrows = get_sample_and_metadata(
                        format_value,
                        local_path_on_curr_worker,
                        exact_sample_needed_res ? 1.0 : session_sample_rate
                    )
                    meta_nrows_on_worker[i] = path_nrows
                    push!(local_samples, path_sample)
                    local_nrows += path_nrows
                end
                total_nrows_res = reduce_and_sync_across(+, local_nrows)

                # If the sample is too small, redo it, getting an exact sample
                if !exact_sample_needed_res && total_nrows_res < max_exact_sample_length
                    exact_sample_needed = true
                    exact_sample_needed_res = true
                else
                    exact_sample_needed = false
                    break
                end
            end
        end
        if Banyan.INVESTIGATING_COLLECTING_SAMPLES
            println("In _remote_table_source on get_worker_idx()=$(get_worker_idx()) with exact_sample_needed=$exact_sample_needed, nrow.(local_samples)=$(DataFrames.nrow.(local_samples))")
        end
        local_sample::DataFrames.DataFrame = isempty(local_samples) ? DataFrames.DataFrame() : vcat(local_samples...)

        # Concatenate local samples and nrows together
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
                res = vcat(sample_per_worker...), vcat(meta_nrows_per_worker...)
                res
            else
                DataFrames.DataFrame(), Int64[]
            end
        else
            sample_per_worker = gather_across(local_sample)
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

            # Convert dataframe to a buffer storing Arrow-serialized data.
            # Then when we receive this on the client side we can simply
            # parse it back into a data frame. This is just to achieve lower
            # latency for retrieving metadata/samples for BDF.jl.
            io = IOBuffer()
            Arrow.write(io, remote_sample_value, compress=:zstd)
            remote_sample_value_arrow = io

            # Construct Sample with the concatenated value, memory usage, and sample rate
            remote_sample_value_memory_usage = total_memory_usage(remote_sample_value)
            total_nbytes_res = if exact_sample_needed
                remote_sample_value_memory_usage
            else
                ceil(Int64, remote_sample_value_memory_usage * session_sample_rate)
            end
            remote_sample_value_nrows = nrow(remote_sample_value)
            if Banyan.INVESTIGATING_COLLECTING_SAMPLES || Banyan.INVESTIGATING_MEMORY_USAGE
                @show total_nrows_res remote_sample_value_nrows
                @show remote_sample_value_memory_usage total_nbytes_res session_sample_rate
            end
            remote_sample_res::Sample = if exact_sample_needed
                # Technically we don't need to be passing in `total_bytes_res`
                # here but we do it because we are anyway computing it to
                # return as the `total_memory_usage` for the `Location` and so
                # we might as well avoid recomputing it in the `Sample`
                # constructors
                ExactSample(remote_sample_value_arrow, total_nbytes_res)
            else
                Sample(remote_sample_value_arrow, total_nbytes_res)
            end
            meta_nrows_on_workers, total_nrows_res, total_nbytes_res, remote_sample_res, empty_sample_value_serialized
        else
            Base.zeros(length(localpaths)), -1, -1, NOTHING_SAMPLE, to_jl_value_contents(DataFrames.DataFrame())
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
            if Banyan.INVESTIGATING_COLLECTING_SAMPLES || Banyan.INVESTIGATING_MEMORY_USAGE
                @show remote_sample_value_nbytes remote_sample_value_nrows total_nrows_res
            end
            total_nbytes_res = ceil(Int64, remote_sample_value_nbytes * total_nrows_res / remote_sample_value_nrows)

            # Update the sample's sample rate and memory usage based on the
            # new # of rows (since the metadata with info about # of rows
            # has been invalidated)
            cached_remote_sample_res.rate = ceil(Int64, total_nrows_res / remote_sample_value_nrows)
            cached_remote_sample_res.memory_usage = ceil(Int64, total_nbytes_res / cached_remote_sample_res.rate)::Int64
            if Banyan.INVESTIGATING_COLLECTING_SAMPLES || Banyan.INVESTIGATING_MEMORY_USAGE
                @show cached_remote_sample_res.rate total_nbytes_res cached_remote_sample_res.memory_usage
            end

            meta_nrows_res, total_nrows_res, total_nbytes_res, cached_remote_sample_res, curr_location.src_parameters["empty_sample"]
        else
            Base.zeros(length(localpaths)), -1, -1, NOTHING_SAMPLE, to_jl_value_contents(DataFrames.DataFrame())
        end
    end

    # If a file does not exist, one of the get_metadata/get_sample functions
    # will error.

    # Write the metadata to an Arrow file
    meta_path = is_main ? get_meta_path(remotepath) : ""
    if curr_parameters_invalid
        # Write `NamedTuple` with metadata to `meta_path` with `Arrow.write`
        Arrow.write(is_main ? meta_path : IOBuffer(), (path=remotepaths, nrows=meta_nrows), compress=:zstd)
    end

    if Banyan.INVESTIGATING_BDF_INTERNET_FILE_NOT_FOUND
        @show (remotepath, meta_path)
    end

    # println("At end of _remote_table_source on get_worker_idx()=$(MPI.Initialized() ? get_worker_idx() : -1)")

    # Return LocationSource
    if is_main
        # Construct the `Location` to return
        if Banyan.INVESTIGATING_COLLECTING_SAMPLES || Banyan.INVESTIGATING_MEMORY_USAGE
            @show total_nbytes
        end
        location_res = LocationSource(
            "Remote",
            Dict(
                # For dispatching the appropriate PF for this format
                "format" => format_string,
                # For constructing the `BanyanDataFrames.DataFrame`'s `nrows::Future` field
                "nrows" => total_nrows,
                # For diagnostics purposes in PFs (partitioning functions)
                "path" => remotepath,
                # For location constructor to use as caching
                "meta_path" => meta_path,
                # For PFs to read from this source
                "empty_sample" => empty_sample
            ),
            total_nbytes,
            remote_sample
        )

        # Write out the updated `Location`
        cache_location(remotepath, location_res, invalidate_sample, invalidate_metadata)

        location_res
    else
        NOTHING_LOCATION
    end
end

RemoteTableSource(remotepath; shuffled=true, metadata_invalid = false, sample_invalid = false, invalidate_metadata = false, invalidate_sample = false, max_exact_sample_length = Banyan.get_max_exact_sample_length())::Location =
    let loc = offloaded(
        _remote_table_source,
        remotepath,
        shuffled,
        metadata_invalid,
        sample_invalid,
        invalidate_metadata,
        invalidate_sample,
        max_exact_sample_length;
        distributed=true
    )
        loc.sample.value = loc.sample.value |> seekstart |> Arrow.Table |> DataFrames.DataFrame
        loc
    end

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