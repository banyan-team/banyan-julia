function read_png(path; add_channelview=false, kwargs...)
    invalidate(path; kwargs...)
    image_loc = RemoteImageSource(path, add_channelview)
    image_loc.src_name == "Remote" || error("$path does not exist")
    invalidate(path; after=true, kwargs...)
    image = Future(;source=image_loc, datatype="Array")
    image_loc_eltype = Banyan.type_from_str(image_loc.src_parameters["eltype"])
    image_loc_size = Banyan.size_from_str(image_loc.src_parameters["size"])
    image_loc_ndims = length(image_loc_size)
    BanyanArrays.Array{image_loc_eltype,image_loc_ndims}(image, Future(image_loc_size))
end

read_jpg(p; kwargs...) = read_png(p; kwargs...)

# TODO: Implement writing

# function write_png(image, path; invalidate_metadata=true, invalidate_sample=true, kwargs...)
#     # TODO: Determine which constructors to use to partition data
#     pt(image, Blocked(image) | Replicated())
#     partitioned_computation(
#         image,
#         destination=RemoteDestination(path; invalidate_metadata=invalidate_metadata, invalidate_sample=invalidate_sample, kwargs...),
#         new_source=_->RemoteSource(path)
#     )
# end

# write_jpg(img, p; kwargs...) = write_png(img, p; kwargs...)
