function read_hdf5(path; kwargs...)
    A_loc = RemoteHDF5Source(path; kwargs...)
    A_loc.src_name == "Remote" || error("$path does not exist")
    A = Future(datatype="Array", source=A_loc)
    A_loc_size = A_loc.src_parameters["size"]
    A_loc_eltype = A_loc.src_parameters["eltype"]
    A_loc_ndims = A_loc.src_parameters["ndims"]
    BanyanArrays.Array{A_loc_eltype,A_loc_ndims}(A, Future(A_loc_size))
end

write_hdf5(A::BanyanArrays.Array, path::String) =
    partitioned_computation(
        BanyanArrays.pts_for_blocked_and_replicated,
        A,
        destination=RemoteHDF5Destination(path),
        new_source=_->RemoteHDF5Source(path)
    )
