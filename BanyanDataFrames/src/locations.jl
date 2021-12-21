get_csv_chunks(localfilepathp) = 
    try
        CSV.Chunks(localfilepathp)
    catch e
        # An ArgumentError may get thrown if the file cannot be
        # read in with the multi-threaded chunked iterator for
        # some reason. See
        # https://github.com/JuliaData/CSV.jl/blob/main/src/context.jl#L583-L641
        # for possible reasons for `ctx.threaded` in CSV.jl
        # code to be false.
        if isa(e, ArgumentError)
            [CSV.File(localfilepathp)]
        else
            throw(e)
        end
    end

function RemoteTableSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    RemoteSource(
        remotepath,
        shuffled=shuffled,
        source_invalid = source_invalid,
        sample_invalid,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    ) do remotepath, remote_source, remote_sample, shuffled
        # `remote_source` and `remote_sample` might be non-null indicating that
        # we need to reuse a location or a sample (but not both since then the
        # `Remote` constructor would have just returned the location).
        # 
        # `shuffled` only applies to sample collection. If we
        # have to collect a sample, then `shuffled` allows us to only read in the
        # first few files. Shuffled basically implies that either the data is
        # shuffled already or the files are similar enough in their file-specific
        # data distribution that we can just randomly select files and read them in
        # sequentially until we have enough for the sample.

        # Get the path ready for using for collecting sample or location info or both
        p = download_remote_path(remotepath)

        # Initialize location parameters with the case that we already have a
        # location
        files = isnothing(remote_source) ? [] : remote_source.files
        totalnrows = isnothing(remote_source) ? 0 : remote_source.nrows
        nbytes = isnothing(remote_source) ? 0 : remote_source.nbytes

        # Determine whether this is a directory
        p_isfile = isfile(p)
        newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
        # AWSS3.jl's S3Path will return true for isdir as long as it ends in
        # a / and will then return empty for readdir. When using S3FS, isdir will
        # actually return false if it isn't a directory but if you call readdir it
        # will fail.
        p_isdir = !p_isfile && isdir(newp_if_isdir)
        if p_isdir
            p = newp_if_isdir
        end

        # Get files to read
        files_to_read_from = if p_isdir
            Random.shuffle(readdir(p))
        elseif p_isfile
            [p]
        else
            []
        end

        # The possibilities:
        # - No location but has sample
        # - No sample but has location, shuffled
        # - No sample, no location, shuffled
        # - No sample but has location, not shuffled
        # - No sample, no location, not shuffled

        # Initialize sample
        exactsample = nothing
        randomsample = nothing
        emptysample = nothing
        already_warned_about_too_large_sample = false
        memory_used_in_sampling = 0

        # A second pass is only needed if there is no sample and the data is
        # shuffled. On this second pass, we only read in some files.

        # Read in location info if we don't have. We also collect the sample along
        # the way unless the data is shuffled - then we wait till after we have a
        # location to read in the location.

        # Loop through files; stop early if we don't need 

        progressbar = Progress(length(files_to_read_from), isnothing(remote_sample) ? "Collecting sample from $remotepath" : "Collecting location information from $remotepath")
        for (fileidx, filep) in enumerate(files_to_read_from)
            # Initialize
            filenrows = 0

            # We download the file because either we need to get the location or
            # we need to collect a sample. If we already have a location and are
            localfilepath = p_isdir ? joinpath(p, filep) : p
            with_downloaded_path_for_reading(localfilepath) do localfilepathp
                # If the data is shuffled, we don't read it it in until we know how
                # many rows there are. We only collect a sample now if we don't
                # already have one and the data isn't shuffled. Because if we
                # don't have a sample and the data _is_ shuffled, we want to
                # compute the sample later.
                if isnothing(remote_sample) && !shuffled
                    chunks = if endswith(localfilepathp, ".csv")
                        get_csv_chunks(localfilepathp)
                    elseif endswith(localfilepathp, ".parquet")
                        Tables.partitions(read_parquet(localfilepathp))
                    elseif endswith(localfilepathp, ".arrow")
                        Arrow.Stream(localfilepathp)
                    else
                        error("Expected .csv or .parquet or .arrow")
                    end

                    # Sample from each chunk
                    for (i, chunk) in enumerate(chunks)
                        chunkdf = DataFrames.DataFrame(chunk, copycols=false)
                        chunknrows = nrow(chunkdf)
                        filenrows += chunknrows
                        if isnothing(remote_source)
                            totalnrows += chunknrows
                        end

                        # Note that we only initialize and/or append to the samples
                        # if this chunk isn't empty. The chunk could be empty and
                        # in the case of CSV.jl, the columns would just be missing
                        # vectors and we wouldn't be able to append to that. So
                        # instead we wait till we get something that we cam
                        # actually use as a sample.

                        # Note that one potential issue is that we might
                        # incorrectly infer schema by assuming that the first
                        # non-empty partition has the schema of the whole data
                        # frame.

                        # Use `chunkdf` to initialize the schema of the sampels
                        # regardless of whethere `chunkdf` has any rows or not.
                        if isnothing(randomsample) && !isempty(chunkdf)
                            randomsample = empty(chunkdf)
                        end
                        if isnothing(exactsample) && !isempty(chunkdf)
                            exactsample = empty(chunkdf)
                        end
                        if isnothing(emptysample)
                            emptysample = empty(chunkdf)
                        end

                        # Append to randomsample
                        # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
                        chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
                        # if any(chunksampleindices)
                        if !isempty(chunkdf) && !isempty(chunksampleindices)
                            append!(randomsample, @view chunkdf[chunksampleindices, :])
                        end

                        # Append to exactsample
                        samplenrows = Banyan.getsamplenrows(totalnrows)
                        if !isempty(chunkdf) && nrow(exactsample) < samplenrows
                            append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
                        end

                        # Warn about sample being too large
                        # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                        memory_used_in_sampling += total_memory_usage(chunkdf)
                        memory_used_in_sampling_total = memory_used_in_sampling + total_memory_usage(exactsample) + total_memory_usage(randomsample)
                        chunkdf = nothing
                        chunk = nothing
                        free_memory = Sys.free_memory()
                        if memory_used_in_sampling_total > cld(free_memory, 4)
                            if !already_warned_about_too_large_sample
                                @warn "Sample of $remotepath is too large ($(format_bytes(memory_used_in_sampling_total))/$(format_bytes(free_memory)) used so far). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                                already_warned_about_too_large_sample = true
                            end
                            GC.gc()
                        end
                    end
                elseif isnothing(remote_source)
                    # Even if the data is shuffled, we will collect location
                    # information that can subsequently be used in the second
                    # pass to read in files until we have a sample.

                    filenrows = if endswith(localfilepathp, ".csv")
                        sum((1 for row in CSV.Rows(localfilepathp)))
                    elseif endswith(localfilepathp, ".parquet")
                        nrows(Parquet.File(localfilepathp))
                    elseif endswith(localfilepathp, ".arrow")
                        Tables.rowcount(Arrow.Table(localfilepathp))
                    else
                        error("Expected .csv or .parquet or .arrow")
                    end
                    totalnrows += filenrows

                    # TODO: Update nbytes if there is no existing location and
                    # we need nbytes for the location even though we don't need
                    # to collect a sample.+++

                    # TODO: Maybe also compute nbytes or perhaps it's okay to just
                    # use the sample to estimate the total memory usage
                end

                # Add to list of file metadata
                if isnothing(remote_source)
                    push!(
                        files,
                        Dict(
                            "path" => p_isdir ? joinpath(remotepath, filep) : remotepath,
                            "nrows" => filenrows,
                        ),
                    )
                end
            end

            next!(progressbar)
        end
        finish!(progressbar)

        # A second (partial) pass over the data if we don't yet have a sample and
        # the data is shuffled. If we didn't have a sample but the data wasn't
        # shuffled we would have collected it already in the first pass.
        if isnothing(remote_sample) && shuffled
            # We should now know exactly how many rows there are and how many
            # to sample.
            samplenrows = Banyan.getsamplenrows(totalnrows)

            # So we can iterate through the files (reverse in case some caching helps
            # us). We append to `randomsample` directly.
            progressbar = Progress(length(files_to_read_from), "Collecting sample from $remotepath")
            for filep in reverse(files_to_read_from)
                localfilepath = p_isdir ? joinpath(p, filep) : p
                with_downloaded_path_for_reading(localfilepath) do localfilepathp
                    chunks = if endswith(localfilepathp, ".csv")
                        get_csv_chunks(localfilepathp)
                    elseif endswith(localfilepathp, ".parquet")
                        Tables.partitions(read_parquet(localfilepathp))
                    elseif endswith(localfilepathp, ".arrow")
                        Arrow.Stream(localfilepathp)
                    else
                        error("Expected .csv or .parquet or .arrow")
                    end

                    # Sample from each chunk
                    for (i, chunk) in enumerate(chunks)
                        # Read in chunk
                        chunkdf = DataFrames.DataFrame(chunk, copycols=false)

                        # Use `chunkdf` to initialize the schema of the sampels
                        # regardless of whethere `chunkdf` has any rows or not.
                        if !isempty(chunkdf) && isnothing(randomsample)
                            randomsample = empty(chunkdf)
                        end
                        if !isempty(chunkdf) && isnothing(exactsample)
                            exactsample = empty(chunkdf)
                        end
                        if isnothing(emptysample)
                            emptysample = empty(chunkdf)
                        end

                        # Append to randomsample; append to exactsample later
                        if !isempty(chunkdf) && nrow(randomsample) < samplenrows
                            append!(randomsample, first(chunkdf, samplenrows - nrow(randomsample)))
                        end

                        # Warn about sample being too large
                        # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                        memory_used_in_sampling += total_memory_usage(chunkdf)
                        memory_used_in_sampling_total = memory_used_in_sampling + 2 * total_memory_usage(randomsample)
                        chunkdf = nothing
                        chunk = nothing
                        free_memory = Sys.free_memory()
                        if memory_used_in_sampling_total > cld(free_memory, 4)
                            if !already_warned_about_too_large_sample
                                @warn "Sample of $remotepath is too large ($(format_bytes(memory_used_in_sampling_total))/$(format_bytes(free_memory)) used so far). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                                already_warned_about_too_large_sample = true
                            end
                            GC.gc()
                        end

                        # Stop as soon as we get our sample
                        if (!isnothing(randomsample) && nrow(randomsample) == samplenrows) || samplenrows == 0
                            break
                        end
                    end
                end

                # Stop as soon as we get our sample. We have reached our sample if
                # there were files to build up a sample and we got how many rows
                # we wanted or the sample should have rows. Because if it should
                # have no rows, then it already had a chance to get a sample and if
                # it didn't, it will just have to settle for a schema-less
                # `DataFrame`.
                if (!isnothing(randomsample) && nrow(randomsample) == samplenrows) || samplenrows == 0
                    break
                end
                
                next!(progressbar)
            end
            finish!(progressbar)

            # In this case, the random sample would also be the exact sample if an
            # exact sample is ever required.
            if totalnrows <= Banyan.get_max_exact_sample_length()
                exactsample = randomsample
            end
        end

        # If there were no files, set the samples to schema-less data frames.
        if !isnothing(randomsample)
            # This is the case where some partition was non-empty and so we can
            # take that schema-ful sample and take the empty version of that.
            emptysample = empty(randomsample)
        elseif isnothing(emptysample)
            # If there were no partitions, we never would have set `emptysample`
            # and so we default to an empty schema-less data frame.
            emptysample = DataFrame()
        end
        # Hopefully there were some partitions and `emptysample` is a
        # schema-ful (having the schema of the first partition) data frame.
        if isnothing(randomsample)
            randomsample = emptysample
        end
        if isnothing(exactsample)
            exactsample = emptysample
        end

        # Adjust sample to have samplenrows
        if isnothing(remote_sample)
            samplenrows = Banyan.getsamplenrows(totalnrows) # Either a subset of rows or the whole thing
            # If we already have enough rows in the exact sample...
            if totalnrows <= Banyan.get_max_exact_sample_length()
                randomsample = exactsample
            end
            # Regardless, expand the random sample as needed...
            if nrow(randomsample) < samplenrows
                append!(randomsample, first(exactsample, samplenrows - nrow(randomsample)))
            end
            # ... and limit it as needed
            if nrow(randomsample) > samplenrows
                randomsample = first(randomsample, samplenrows)
            end
        end

        # If the sample is a PooledArray or CategoricalArray, convert it to a
        # simple array so we can correctly compute its memory usage.
        for s in [emptysample, randomsample]
            for pn in Base.propertynames(s)
                sc = s[!, pn]
                if !(sc isa Array)
                    s[!, pn] = Banyan.convert_to_unpooled(sc)
                end
            end
        end

        # Re-compute the number of bytes. Even if we are reusing a location, we go
        # ahead and re-compute the sample. Someone may have written data with
        # `invalidate_source=false` but `shuffled=true` (perhaps the only thing
        # they changed was adding a column and somehow they ensured that the same
        # data got written to the same files) and so we want to estimate the total
        # memory usage using a newly collected sample but the same `totalnrow` we
        # already know from the reused location.
        remote_sample_value = isnothing(remote_sample) ? randomsample : remote_sample.value
        remote_sample_rate = totalnrows > 0 ? totalnrows / nrow(remote_sample_value) : 1.0
        nbytes = ceil(total_memory_usage(remote_sample_value) * remote_sample_rate)

        # Load metadata for reading
        # If we're not using S3FS, the files might be empty because `readdir`
        # would just return empty but `p_isdir` might be true because S3Path will
        # say so for pretty much anything unless it's a file. So we might end up
        # creating a location source with a path that doesn't exist and an empty
        # list of files. But that's actually okay because in the backend, we will
        # check isdir before we ever try to readdir from something. We will check
        # both isfile and isdir. So for example, we won't read in an HDF5 file if
        # it is not a file. And we won't call readdir if it isn't isdir and that
        # works fine in the backend since we use S3FS there.
        loc_for_reading, metadata_for_reading = if !isempty(files) || p_isdir # empty directory can still be read from
            (
                "Remote",
                Dict(
                    "path" => remotepath,
                    "files" => files,
                    "nrows" => totalnrows,
                    "nbytes" => nbytes,
                    "emptysample" => to_jl_value_contents(empty(randomsample)),
                    "format" => uppercase(splitext(remotepath))
                )
            )
        else
            ("None", Dict{String,Any}())
        end

        # Get remote sample
        if isnothing(remote_sample)
            remote_sample = if isnothing(loc_for_reading)
                Sample()
            elseif totalnrows <= Banyan.get_max_exact_sample_length()
                ExactSample(randomsample, total_memory_usage = nbytes)
            else
                Sample(randomsample, total_memory_usage = nbytes)
            end
        else
            # Adjust sample properties if this is a reused sample. But we only do
            # this if we are reading from this location. If we are writing some
            # value to some location that doesn't exist yet, we don't want to
            # change the sample or the sample rate.
            # TODO: Don't set the sample or sample properties if we are merely trying to overwrite something.
            setsample!(remote_sample, :memory_usage, ceil(nbytes / remote_sample_rate))
            setsample!(remote_sample, :rate, remote_sample_rate)
        end

        # In the process of collecting this sample, we have called functions in
        # `utils_s3fs.jl` for reading from remote locations. These functions save
        # files in /tmp and so we need to cleanup (delete) these files.
        # TODO: Surround this function by a try-catch so that if this fails, we
        # will cleanup anyway.
        cleanup_tmp()

        # Construct location with metadata
        LocationSource(
            loc_for_reading,
            metadata_for_reading,
            remote_sample,
        )
    end
end

function RemoteTableDestination(remotepath; invalidate_source = true, invalidate_sample = true)::Location
    RemoteDestination(p, invalidate_source = invalidate_source, invalidate_sample = invalidate_sample) do remotepath
        # Load metadata for writing
        # NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
        # or CSV dataset is desired to be created
        loc_for_writing, metadata_for_writing = (
            "Remote",
            Dict(
                "path" => remotepath,
                "files" => [],
                "nrows" => 0,
                "nbytes" => 0,
                "format" => uppercase(splitext(remotepath))
            )
        )

        LocationDestination(loc_for_writing, metadata_for_writing)
    end
end