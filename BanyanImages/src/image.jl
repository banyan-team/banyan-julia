function read_png(path; kwargs...)
    image_loc = RemoteImageSource(path; kwargs...)
    image_loc.src_name == "Remote" || error("$path does not exist")
    image = Future(;source=image_loc, datatype="Array")
    BanyanArrays.Array{image_loc.eltype,image_loc.ndims}(image, Future(image_loc.src_parameters["size"]))
end

read_jpg(p; kwargs...) = read_png(p; kwargs...)

# function write_png(image, path; invalidate_source=true, invalidate_sample=true, kwargs...)
#     # TODO: Determine which constructors to use to partition data
#     pt(image, Blocked(image) | Replicated())
#     partitioned_computation(
#         image,
#         destination=RemoteDestination(path; invalidate_source=invalidate_source, invalidate_sample=invalidate_sample, kwargs...),
#         new_source=_->RemoteSource(path)
#     )
# end

# write_jpg(img, p; kwargs...) = write_png(img, p; kwargs...)
