function get_image_format(path)
    if endswith(path, ".png")
        "png"
    elseif endswith(path, ".jpg")
        "jpg"
    else
        error("Unsupported file format. Must be jpg or png")
    end
end

MAX_EXACT_SAMPLE_NUM_IMAGES = 100


function RemoteImageSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false, add_channelview=false)::Location
    RemoteSource(
        remotepath,
        shuffled = shuffled,
        source_invalid = source_invalid,
        sample_invalid = sample_invalid,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    ) do remotepath, remote_source, remote_sample, shuffled

        # Initialize parameters if location is already cached
        files = isnothing(remote_source) ? [] : remote_source.files  # list, Tuple
        nimages = isnothing(remote_source) ? 0 : remote_source.nimages
        nbytes = isnothing(remote_source) ? 0 : remote_source.nbytes
        ndims = isnothing(remote_source) ? 0 : remote_source.ndims
        datasize = isnothing(remote_source) ? () : remote_source.size
        dataeltype = isnothing(remote_source) ? "" : remote_source.eltype
        format = isnothing(remote_source) ? "" : remote_source.format  # png, jpg


        # TODO: I think if the above parameters were cached, they still get
        # read in again

        # Remote path is either
        #   a single file path,
        #   a list of file paths,
        #   a 2-tuple of (1) an iterable range and (2) function that operates
        #       on each iterated element and returns a single path
        #   a 3-tuple of (1) an object, (2) an iterable range, and (3) a function
        #       that operates on two arguments where one is the object and the
        #       other is each iterated element and return a single path
        # The file paths can either be S3 or HTTP
        if isa(remotepath, Tuple)
            # Create a generator here for sampling
            if length(remotepath) > 3 || length(remotepath) < 2
                error("Remotepath is invalid")
            elseif length(remotepath) == 3
                files_to_read_from = (
                    remotepath[3](remotepath[1], idx...)
                    for idx in remotepath[2]
                )
            else  # 2
                files_to_read_from = (
                    remotepath[2](idx...)
                    for idx in remotepath[1]
                )
            end
        else  # single path or list of paths

            if !isa(remotepath, Base.Array)  # single path
                p = Banyan.download_remote_path(remotepath)

                # Determine if this is a directory
                p_isfile = isfile(p)
                newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
                p_isdir = !p_isfile && isdir(newp_if_isdir)
                if p_isdir
                    p = newp_if_isdir
                end

                # Get files to read
                files_to_read_from = if p_isdir
                    [joinpath(remotepath, filep) for filep in Random.shuffle(readdir(p))]
                elseif p_isfile
                    [remotepath]
                else
                    []
                end
            else
                files_to_read_from = remotepath
            end
        end

        # Determine nimages
        if isnothing(remote_source)
            iterator_size = Iterators.IteratorSize(files_to_read_from)
            if iterator_size == Base.IsInfinite()
                error("Infinite generators are not supported")
            elseif iterator_size == Base.SizeUnknown()
                nimages = sum(1 for _ in files_to_read_from)
            else  # length can be predetermined
                nimages = length(files_to_read_from)
            end
        end
        meta_collected = false

        # Initialize sample
        randomsample = nothing

        if isnothing(remote_sample)

            samplesize = (nimages <= MAX_EXACT_SAMPLE_NUM_IMAGES) ? nimages : ceil(nimages / get_session().sample_rate)
            nbytes_of_sample = 0

            progressbar = Progress(length(files_to_read_from), "Collecting sample from $remotepath")
            for filep in files_to_read_from
                p = download_remote_path(filep)
                with_downloaded_path_for_reading(p) do pp

                    # Load file and collect metadata and sample
                    image = load(pp)
                    if add_channelview
                        image = ImageCore.channelview(image)
                    end

                    if isnothing(remote_source) && !meta_collected
                        nbytes = length(image) * sizeof(eltype(image)) * nimages
                        ndims = length(size(image)) + 1 # first dim
                        dataeltype = eltype(image)
                        datasize = (nimages, size(image)...)
                        format = get_image_format(pp)
                        meta_collected = true
                    end
                    nbytes_of_sample += length(image) * sizeof(eltype(image))

                    if isnothing(randomsample)
                        randomsample = []
                    end
                    if length(randomsample) < samplesize
                        push!(randomsample, reshape(image, (1, size(image)...)))  # add first dimension
                    end

                    # TODO: Warn about sample being too large

                end

                # Stop as soon as we get our sample
                if (!isnothing(randomsample) && size(randomsample)[1] == samplesize) || samplesize == 0
                    break
                end

                next!(progressbar)
            end
            finish!(progressbar)

            if isnothing(remote_source)
                # Estimate nbytes based on the sample
                nbytes = (nbytes_of_sample / length(randomsample)) * length(files_to_read_from)
            end

        elseif isnothing(remote_source)
            # No location, but has sample
            # In this case, read one random file to collect metadata
            # We assume that all files have the same nbytes and ndims

            filep = collect(Iterators.take(Iterators.reverse(files_to_read_from), 1))[1]
            p = download_remote_path(filep)
            with_downloaded_path_for_reading(p) do pp

                # Load file and collect metadata and sample
                image = load(pp)
                if add_channelview
                    image = ImageCore.channelview(image)
                end

                nbytes = length(image) * sizeof(eltype(image)) * nimages
                ndims = length(size(image)) + 1 # first dim
                dataeltype = eltype(image)
                datasize = (nimages, size(image)...)
                format = get_image_format(pp)
            end
        end

        # Serialize generator
        if isnothing(remote_source)
            files = isa(remotepath, Tuple) ? Banyan.to_jl_value_contents(remotepath) : files_to_read_from
        end

        loc_for_reading, metadata_for_reading = if !isnothing(files) && !isempty(files)
            (
                "Remote",
                Dict(
                    "files" => files,  # either a serialized tuple or list of filepaths
                    "nimages" => nimages,
                    "nbytes" => nbytes,  # assume all files have same size
                    "ndims" => ndims,
                    "size" => datasize,
                    "eltype" => dataeltype,
                    "format" => format,
                    "add_channelview" => add_channelview
                ),
            )
        else
            ("None", Dict{String,Any}())
        end

        # Get the remote sample
        if isnothing(remote_sample)
            randomsample = cat(randomsample..., dims=1) # Put to correct shape
            remote_sample = if isnothing(loc_for_reading)
                Sample()
            elseif nimages <= MAX_EXACT_SAMPLE_NUM_IMAGES
                ExactSample(randomsample, total_memory_usage = nbytes)
            else
                Sample(randomsample, total_memory_usage = nbytes)
            end
        end

        # Construct location with metadata
        LocationSource(
            loc_for_reading,
            metadata_for_reading,
            ceil(Int, nbytes),
            remote_sample,
        )
    end
end


# function RemoteImageDestination(remotepath; invalidate_source = true, invalidate_sample = true)::Location
#     RemoteDestination(p, invalidate_source = invalidate_source, invalidate_sample = invalidate_sample) do remotepath
        
#         # NOTE: Path for writing must be a directory
#         remotepath = endswith(string(remotepath), "/") ? p : (remotepath * "/")
        
#         loc_for_writing, metadata_for_writing = (
#             "Remote",
#             Dict(
#                 "path" => remotepath,
#                 # TODO: Dynamically determine format
#                 "format" => "png"
#             )
#         )

#         LocationDestination(loc_for_writing, metadata_for_writing)
#     end
# end