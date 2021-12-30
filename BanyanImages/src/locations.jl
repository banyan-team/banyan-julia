function RemotePNGSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    RemoteSource(
        remotepath,
        shuffled=shuffled,
        source_invalid = source_invalid,
        sample_invalid,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    ) do remotepath, remote_source, remote_sample, 

        # Initialize parameters if location is already cached
        files = isnothing(remote_source) ? [] : remote_source.files
        nbytes = isnothing(remote_source) ? 0 : remote_source.nbytes
        dataeltype = isnothing(remote_source) ? "" : remote_source.dataeltype
        format = isnothing(remote_source) ? "" : remote_source.format


        # Remote path is either a single file path, a list of file paths,
        # or a generator. The file paths can either be S3 or HTTP
        if isa(remotepath, Base.Generator)
            files = remotepath
            format = "generator"
        else

            if !isa(remotepath, Array)
                p = download_remote_path(remotepath)

                # Determine if this is a directory
                p_isfile = isfile(p)
                newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
                p_isdir = !p_isfile && isdir(newp_if_isdir)
                if p_isdir
                    p = newp_if_isdir
                end

                # Get files to read
                files = if p_isdir
                    [joinpath(remotepath, filep) for filep in Random.shuffle(readdir(p))]
                elseif p_isfile
                    [p]
                else
                    []
                end
            else
                files = remotepath
            end
            format = "png"
        end

        # Initialize sample
        randomsample = nothing

        if isnothing(remote_sample)

            samplesize = Banyan.get_max_exact_sample_length()
            nbytes_of_sample = 0

            progressbar = Progress(length(files), "Collecting sample from $remotepath")
            for filep in files
                with_downloaded_path_for_reading(filep) do filep

                    # Load file and collect metadata and sample
                    image = load(filep)

                    if isnothing(remote_source)
                        ndims = length(size(image))
                        dataeltype = eltype(image)
                    end
                    nbytes_of_sample += length(image) * sizeof(eltype(image))

                    if isnothing(randomsample)
                        randomsample = []
                    end
                    if length(randomsample) < samplesize
                        append!(randomsample, image)
                    end

                    # TODO: Warn about sample being too large

                end

                # Stop as soon as we get our sample
                if (!isnothing(randomsample) && length(randomsample) == samplesize) || samplesize == 0
                    break
                end

                next!(progressbar)
            end
            finish!(progressbar)

            if isnothing(remote_source)
                # Estimate nbytes based on the sample
                nbytes = (nbytes_of_sample / length(randomsample)) * length(files)
            end

        elseif isnothing(remote_source)
            # No location, but has sample
            # In this case, read one random file to collect metadata
            # We assume that all files have the same nbytes and ndims

            filep = files_to_read_from[end]
            with_downloaded_path_for_reading(filep) do filep

                # Load file and collect metadata and sample
                image = load(filep)

                nbytes = length(image) * sizeof(eltype(image)) * length(files)
                dataeltype = eltype(image)
            end
        end

        # Serialize generator
        files = format == "generator" ? Banyan.to_jl_value_contents(files) : files

        loc_for_reading, metadata_for_reading = if dataset_to_read_from_exists
            (
                "Remote",
                Dict(
                    "path" => remotepath,
                    "files" => files,  # either a serialized generator or list of filepaths
                    "nbytes" => nbytes,  # assume all files have same size
                    "dataeltype" => dataeltype,
                    "format" => format
                ),
            )
        else
            ("None", Dict{String,Any}())
        end

        # Get the remote sample
        if isnothing(remote_sample)
            remote_sample = if isnothing(loc_for_reading)
                Sample()
            elseif length(files) <= Banyan.get_max_exact_sample_length()
                ExactSample(randomsample, total_memory_usage = nbytes)
            else
                Sample(randomsample, total_memory_usage = nbytes)
            end
        end

        # Construct location with metadata
        LocationSource(
            loc_for_reading,
            metadata_for_reading,
            remote_sample,
        )
    end
end


function RemotePNGDestination(remotepath; invalidate_source = true, invalidate_sample = true)::Location
    RemoteDestination(p, invalidate_source = invalidate_source, invalidate_sample = invalidate_sample) do remotepath
        
        # NOTE: Path for writing must be a directory
        remotepath = endswith(string(remotepath), "/") ? p : (remotepath * "/")
        
        loc_for_writing, metadata_for_writing = (
            "Remote",
            Dict(
                "path" => remotepath,
                "format" => "png"
            )
        )

        LocationDestination(loc_for_writing, metadata_for_writing)
    end
end