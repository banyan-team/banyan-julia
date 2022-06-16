# function get_image_format(path::String)::String
#     if endswith(path, ".png")
#         "png"
#     elseif endswith(path, ".jpg") || endswith(path, ".jpeg")
#         "jpg"
#     else
#         error("Unsupported file format; must be jpg or png")
#     end
# end

# MAX_EXACT_SAMPLE_NUM_IMAGES = 100

# function get_remote_image_source(
#     remotepath,
#     remote_source::Location,
#     remote_sample::Sample,
#     shuffled::Bool
# )::Location
#     # Initialize parameters if location is already cached
#     files::Union{Base.Vector{String},Tuple,String} = isnothing(remote_source) ? String[] : remote_source.src_parameters["files"]  # list, Tuple
#     nimages::Int64 = isnothing(remote_source) ? 0 : remote_source.src_parameters["nimages"]
#     nbytes::Int64 = isnothing(remote_source) ? 0 : remote_source.src_parameters["nbytes"]
#     ndims::Int64 = isnothing(remote_source) ? 0 : remote_source.src_parameters["ndims"]
#     datasize = isnothing(remote_source) ? () : remote_source.src_parameters["size"]
#     dataeltype = isnothing(remote_source) ? "" : remote_source.src_parameters["eltype"]
#     format::String = isnothing(remote_source) ? "" : remote_source.src_parameters["format"]  # png, jpg
#     add_channelview::Bool = shuffled


#     # TODO: I think if the above parameters were cached, they still get
#     # read in again

#     # Remote path is either
#     #   a single file path,
#     #   a list of file paths,
#     #   a 2-tuple of (1) an iterable range and (2) function that operates
#     #       on each iterated element and returns a single path
#     #   a 3-tuple of (1) an object, (2) an iterable range, and (3) a function
#     #       that operates on two arguments where one is the object and the
#     #       other is each iterated element and return a single path
#     # The file paths can either be S3 or HTTP
#     if isa(remotepath, Tuple)
#         # Create a generator here for sampling
#         if length(remotepath) > 3 || length(remotepath) < 2
#             error("Remotepath is invalid")
#         elseif length(remotepath) == 3
#             files_to_read_from = (
#                 remotepath[3](remotepath[1], idx...)
#                 for idx in remotepath[2]
#             )
#         else  # 2
#             files_to_read_from = (
#                 remotepath[2](idx...)
#                 for idx in remotepath[1]
#             )
#         end
#     else  # single path or list of paths

#         if !isa(remotepath, Base.Array)  # single path
#             p = Banyan.download_remote_path(remotepath)

#             # Determine if this is a directory
#             p_isfile = isfile(p)
#             newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
#             p_isdir = !p_isfile && isdir(newp_if_isdir)
#             if p_isdir
#                 p = newp_if_isdir
#             end

#             # Get files to read
#             files_to_read_from = if p_isdir
#                 map(filep -> joinpath(remotepath, filep), Random.shuffle(readdir(p)))
#             elseif p_isfile
#                 String[remotepath]
#             else
#                 String[]
#             end
#         else
#             files_to_read_from = remotepath
#         end
#     end

#     # Determine nimages
#     if isnothing(remote_source)
#         iterator_size = Iterators.IteratorSize(files_to_read_from)
#         if iterator_size == Base.IsInfinite()
#             error("Infinite generators are not supported")
#         elseif iterator_size == Base.SizeUnknown()
#             nimages = sum(1 for _ in files_to_read_from)
#         else  # length can be predetermined
#             nimages = length(files_to_read_from)
#         end
#     end
#     meta_collected = false

#     # Initialize sample
#     randomsample = nothing

#     if isnothing(remote_sample)

#         samplesize = (nimages <= MAX_EXACT_SAMPLE_NUM_IMAGES) ? nimages : ceil(Int64, nimages / get_session().sample_rate)
#         nbytes_of_sample = 0

#         progressbar = Progress(length(files_to_read_from), "Collecting sample from $remotepath")
#         for filep in files_to_read_from
#             p = download_remote_path(filep)
#             pp::String = get_downloaded_path(p)

#             # Load file and collect metadata and sample
#             image = load(pp)
#             if add_channelview
#                 image = ImageCore.channelview(image)
#             end

#             if isnothing(remote_source) && !meta_collected
#                 nbytes = length(image) * sizeof(eltype(image)) * nimages
#                 ndims = length(size(image)) + 1 # first dim
#                 dataeltype = eltype(image)
#                 datasize = (nimages, size(image)...)
#                 format = get_image_format(pp)
#                 meta_collected = true
#             end
#             nbytes_of_sample += length(image) * sizeof(eltype(image))

#             if isnothing(randomsample)
#                 randomsample = []
#             end
#             if length(randomsample) < samplesize
#                 push!(randomsample, reshape(image, (1, size(image)...)))  # add first dimension
#             end

#             destroy_downloaded_path(pp)

#             # TODO: Warn about sample being too large

#             # Stop as soon as we get our sample
#             if (!isnothing(randomsample) && size(randomsample)[1] == samplesize) || samplesize == 0
#                 break
#             end

#             next!(progressbar)
#         end
#         finish!(progressbar)

#         if isnothing(remote_source)
#             # Estimate nbytes based on the sample
#             nbytes = (nbytes_of_sample / length(randomsample)) * length(files_to_read_from)
#         end

#     elseif isnothing(remote_source)
#         # No location, but has sample
#         # In this case, read one random file to collect metadata
#         # We assume that all files have the same nbytes and ndims

#         filep = Base.collect(Iterators.take(Iterators.reverse(files_to_read_from), 1))[1]
#         p = download_remote_path(filep)
#         pp = get_downloaded_path(p)

#         # Load file and collect metadata and sample
#         image = load(pp)
#         if add_channelview
#             image = ImageCore.channelview(image)
#         end

#         nbytes = length(image) * sizeof(eltype(image)) * nimages
#         ndims = length(size(image)) + 1 # first dim
#         dataeltype = eltype(image)
#         datasize = (nimages, size(image)...)
#         format = get_image_format(pp)

#         destroy_downloaded_path(pp)
#     end

#     # Serialize generator
#     if isnothing(remote_source)
#         files = remotepath isa Tuple ? Banyan.to_jl_value_contents(remotepath) : files_to_read_from
#     end

#     empty_part_size = (0, (datasize[2:end])...)

#     loc_for_reading, metadata_for_reading = if !isnothing(files) && !isempty(files)
#         (
#             "Remote",
#             Dict{String,Any}(
#                 "files" => files,  # either a serialized tuple or list of filepaths
#                 "nimages" => nimages,
#                 "nbytes" => nbytes,  # assume all files have same size
#                 "ndims" => ndims,
#                 "size" => datasize,
#                 "eltype" => dataeltype,
#                 "emptysample" => to_jl_value_contents(Base.Array{dataeltype}(undef, empty_part_size)),
#                 "format" => format,
#                 "add_channelview" => add_channelview
#             ),
#         )
#     else
#         ("None", Dict{String,Any}())
#     end

#     # Get the remote sample
#     if isnothing(remote_sample)
#         randomsample = cat(randomsample..., dims=1) # Put to correct shape
#         remote_sample = if isnothing(loc_for_reading)
#             Sample()
#         elseif nimages <= MAX_EXACT_SAMPLE_NUM_IMAGES
#             ExactSample(randomsample, nbytes)
#         else
#             Sample(randomsample, nbytes)
#         end
#     end

#     # Construct location with metadata
#     LocationSource(
#         loc_for_reading,
#         metadata_for_reading,
#         ceil(Int64, nbytes),
#         remote_sample,
#     )
# end

# TODO: Fix this so that you don't need to invalidate your metadata each time you use
# a different cluster

localtoremote(path) = replace(path, "/home/ec2-user/s3/" => "s3://")

function getpaths(remotepath::String)::Base.Vector{String}
    # directory
    if startswith(path, "s3://")
        localpath = replace(path, "s3://" => "/home/ec2-user/s3/")
        if isdir(localpath)
            localpaths = readdir(localpath, join=true)
            map(localtoremote, localpaths)
        else
            String[path]
        end
    else
        String[path]
    end
end

# list of paths
getpaths(remotepath::Base.Vector)::Base.Vector{String} = remotepath

function getpaths(remotepath::Tuple)::Base.Vector{String}
    # tuple storing info about a generator
    files = String[]
    if length(remotepath) > 3 || length(remotepath) < 2
        error("Remotepath is invalid")
    elseif length(remotepath) == 3
        for idx in remotepath[2]
            push!(files, Base.invokelatest(remotepath[3], (remotepath[1], idx...)))
        end
    else
        for idx in remotepath[1]
            push!(files, Base.invokelatest(remotepath[2], (idx...)))
        end
    end
    files
end

load_retry = retry(load; delays=Base.ExponentialBackOff(; n=5))
_load_image(path_on_worker::String) = load_retry(path_on_worker)
_load_image_and_add_channelview(path_on_worker::String) = load_retry(path_on_worker) |> ImageCore.channelview

# function _get_image_metadata(image)
#     nbytes = length(image) * sizeof(eltype(image))
#     ndims = length(size(image)) + 1 # first dim
#     dataeltype = eltype(image)
#     datasize = (nimages, size(image)...)
#     nbytes, ndims, dataeltype, datasize
# end

_reshape_image(image) = reshape(image, (1, size(image)...))

function _remote_image_source(
    remotepath,
    metadata_invalid,
    sample_invalid,
    invalidate_metadata,
    invalidate_sample,
    add_channelview
)
    # Get session information
    session_sample_rate = get_session().sample_rate
    worker_idx, nworkers = get_worker_idx(), get_nworkers()
    is_main = worker_idx == 1

    # Get current location
    println("Before get_cached_location on get_worker_idx()=$(get_worker_idx()) with remotepath=$remotepath")
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location((remotepath, add_channelview), metadata_invalid, sample_invalid)
    if !curr_parameters_invalid && !curr_sample_invalid
        return curr_location
    end

    # Remote path is either
    #   a single file path,
    #   a list of file paths,
    #   a 2-tuple of (1) an iterable range and (2) function that operates
    #       on each iterated element and returns a single path
    #   a 3-tuple of (1) an object, (2) an iterable range, and (3) a function
    #       that operates on two arguments where one is the object and the
    #       other is each iterated element and return a single path

    # Iterable object that iterates over local paths
    meta_path = if !curr_parameters_invalid
        curr_location.src_parameters["meta_path"]::String
    else
        is_main ? get_meta_path((remotepath, add_channelview)) : ""
    end
    if is_main && curr_parameters_invalid
        localpaths::Base.Vector{String} = getpaths(remotepath)
        Arrow.write(meta_path, (path=localpaths,))
    end
    if !curr_parameters_invalid
        # Now the banyan_metadata directory has surely been created so we can
        # get_meta_path on all workers.
        meta_path = get_meta_path((remotepath, add_channelview))
    end
    sync_across()

    # Load in the metadata and get the # of images
    meta_path = if !curr_parameters_invalid
        curr_location.src_parameters["meta_path"]::String
    else
        # Now it is safe to call this on all workers because the meta directory
        # has definitely been created now
        get_meta_path((remotepath, add_channelview))
    end
    println("Before loading $meta_path on get_worker_idx()=$(get_worker_idx())")
    meta_table = Arrow_Table_retry(meta_path)
    println("Loaded table on get_worker_idx()=$(get_worker_idx())")
    nimages = Tables.rowcount(meta_table)
    println("Loaded rowcount on get_worker_idx()=$(get_worker_idx())")
    
    # Read in images on each worker. We need to read in at least one image
    # regardless of whether we want to get the sample or the metadata
    exact_sample_needed = nimages < 10
    need_to_parallelize = nimages >= 10
    total_num_images_to_read_in = if curr_sample_invalid
        exact_sample_needed ? nimages : cld(nimages, session_sample_rate)
    else
        # We still have to read in an image even if we have a valid sample
        # because to get the metadata we need at least one image.
        1
    end
    samples_on_workers = if is_main || need_to_parallelize
        # If we don't need to paralellize then we are only reading on the main
        # worker amd we don't gather across.
        images_range_on_worker = need_to_parallelize ? split_len(total_num_images_to_read_in, worker_idx, nworkers) : 1:1
        paths_on_worker = map(getpath, meta_table.path[images_range_on_worker])
        images = map(add_channelview ? _load_image_and_add_channelview : _load_image, paths_on_worker)
        sample_on_worker = map(_reshape_image, images)
        # sample_on_worker is an array of images
        need_to_parallelize ? gather_across(sample_on_worker) : [sample_on_worker]
        # result is an array of arrays of images
    else
        []
    end

    if is_main
        # Get the sample and the metadata (we can compute this regardless of
        # whether we need the sample or the metadata
        # though if we only need the sample we don't technically need the
        # metadata)
        remote_sample_value = cat(vcat(samples_on_workers...)..., dims=1)
        ndims_res = ndims(remote_sample_value)
        dataeltype_res = eltype(remote_sample_value)
        nbytes_res = cld(length(remote_sample_value) * sizeof(dataeltype_res) * nimages, total_num_images_to_read_in)
        datasize_res = indexapply(nimages, size(remote_sample_value), 1)
        remote_sample = if curr_sample_invalid
            exact_sample_needed ? ExactSample(remote_sample_value, nbytes_res) : Sample(remote_sample_value, nbytes_res)
        else
            curr_location.sample
        end

        # Construct location with metadata
        location_res = LocationSource(
            "Remote",
            if curr_parameters_invalid
                empty_part_size = (0, (datasize_res[2:end])...)
                Dict{String,Any}(
                    "meta_path" => meta_path,
                    "nimages" => nimages,
                    "nbytes" => nbytes_res,  # NOTE: We assume all files have same size
                    "ndims" => ndims_res,
                    "size" => datasize_res,
                    "eltype" => dataeltype_res,
                    "empty_sample" => to_jl_value_contents(Base.Array{dataeltype_res}(undef, empty_part_size)),
                    "add_channelview" => add_channelview,
                    "format" => "image"
                )
            else
                curr_location.src_parameters
            end,
            nbytes_res,
            remote_sample,
        )
        cache_location(remotepath, location_res, invalidate_sample, invalidate_metadata)
        location_res
    else
        INVALID_LOCATION
    end
end

function RemoteImageSource(remotepath; metadata_invalid = false, sample_invalid = false, invalidate_metadata = false, invalidate_sample = false, add_channelview=false)::Location
    offloaded(
        _remote_image_source,
        remotepath,
        metadata_invalid,
        sample_invalid,
        invalidate_metadata,
        invalidate_sample,
        add_channelview;
        distributed=true
    )
end

# TODO: Implement writing

# function RemoteImageDestination(remotepath; invalidate_metadata = true, invalidate_sample = true)::Location
#     RemoteDestination(p, invalidate_metadata = invalidate_metadata, invalidate_sample = invalidate_sample) do remotepath
        
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