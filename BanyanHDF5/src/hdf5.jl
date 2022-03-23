function read_hdf5(path; kwargs...)
    A_loc = RemoteHDF5Source(path; kwargs...)
    A_loc.src_name == "Remote" || error("$path does not exist")
    if is_debug_on()
        # @show A_loc.src_parameters
        # @show A_loc.size
    end
    A = Future(datatype="Array", source=A_loc)
    BanyanArrays.Array{A_loc.eltype,A_loc.ndims}(A, Future(A_loc.size))
end

function write_hdf5(
    A::BanyanArrays.Array,
    path::String;
    invalidate_source::Bool = true,
    invalidate_sample::Bool = true
)
    # # A_loc = Remote(pathname, mount)
    # destined(A, Remote(path, delete_from_cache=true))
    # mutated(A)
    # # This doesn't rely on any sample properties so we don't need to wrap this
    # # in a `partitioned_with` to delay the PT construction to after sample
    # # properties are computed.
    # pt(A, Blocked(A) | Replicated())
    # # partition(A, Replicated()) # TODO: Use semicolon for keyword args
    # # Distributed will allow for balanced=true|false and any divisions or key
    # # but the PT library should be set up such that you can't split if
    # # divisions or key are not provided or balanced=false
    # # partition(A, Blocked())
    # # for axis in 1:min(4, ndims(A))
    # #     # Partition with distribution of either balanced, grouped, or unknown
    # #     partition(A, Blocked(key=a), mutated=true)
    # # end
    # @partitioned A begin end
    # compute(A)
    partitioned_computation(
        A,
        destination=RemoteHDF5Destination(
            path;
            invalidate_source=invalidate_source,
            invalidate_sample=invalidate_sample
        ),
        new_source=_->RemoteHDF5Source(path)
    ) do f::Future
        pt(f, Blocked(f) | Replicated())
    end
end