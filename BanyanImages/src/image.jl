function read_png(path; kwargs...)
    image_loc = RemotePNGSource(path; kwargs...)
    image_loc.src_name == "Remote" || error("$path does not exist")
    image = Future(datatype="Array", source=image_loc)
    Array{image_loc.eltype,image_loc.ndims}(image, Future(image_loc.size))
end


function write_png(image, path; invalidate_source=true, invalidate_sample=true, kwargs...)
    # TODO: Determine which constructors to use to partition data
    pt(image, Blocked(image) | Replicated())
    partitioned_computation(
        image,
        destination=RemoteDestination(path; invalidate_source=invalidate_source, invalidate_sample=invalidate_sample, kwargs...),
        new_source=_->RemoteSource(path)
    )
end