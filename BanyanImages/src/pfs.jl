function ReadBlockImage(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    println("At start of ReadBlockImage")
    # path = Banyan.getpath(loc_params["path"]) ? isa(loc_params["path"], String) : path
    files = loc_params["files"]
    # ndims = loc_params["ndims"]
    # nbytes = loc_params["nbytes"]
    nimages = loc_params["nimages"]
    datasize = loc_params["size"]
    empty_sample = Banyan.from_jl_value_contents(loc_params["emptysample"])
    # dataeltype = loc_params["eltype"]
    # file_extension = "." * loc_params["format"]
    add_channelview = loc_params["add_channelview"]

    # files is either a list of file paths or a serialized tuple containing
    # information to construct a generator
    if !isa(files, Base.Array)
        iter_info = Banyan.from_jl_value_contents(files)
        # Construct a generator
        if length(iter_info) > 3 || length(iter_info) < 2
            error("Remotepath is invalid")
        elseif length(iter_info) == 3
            files = (
                Base.invokelatest(iter_info[3], (iter_info[1], idx...))
                for idx in iter_info[2]
            )
        else  # 2
            files = (
                Base.invokelatest(iter_info[2], (idx...))
                for idx in iter_info[1]
            )
        end
    end

    # Identify the range of indices of files for the batch currently
    # being processed by this worker
    filerange = Banyan.split_len(nimages, batch_idx, nbatches, comm)

    if isa(files, Base.Generator)
        # Get the subset of the iterator which corresponds to the range
        # that this workers is going to process
        files_sub = Iterators.take(Iterators.drop(files, filerange.start - 1), filerange.stop - filerange.start + 1)
    else
        files_sub = view(files, filerange)
    end

    part_size = (length(files_sub), (datasize)[2:end]...)
    empty_sample_eltype = eltype(empty_sample)
    images = Base.Array{empty_sample_eltype}(undef, part_size)
    for (i, f) in enumerate(files_sub)
        filepath = Banyan.getpath(f, comm)
        image = load(filepath)
        if add_channelview
            image = ImageCore.channelview(image)
            images[i, :, :, :] = image
        else
            images[i, :, :] = image
        end
    end
    println("At end of ReadBlockImage")
    images
end


# function WriteImage(
#     src,
#     part,
#     params,
#     batch_idx::Integer,
#     nbatches::Integer,
#     comm::MPI.Comm,
#     loc_name,
#     loc_params,
# )
#     # Get rid of splitting divisions if they were used to split this data into
#     # groups
#     splitting_divisions = Banyan.get_splitting_divisions()
#     delete!(splitting_divisions, part)

#     # Get path of directory to write to
#     path = loc_params["path"]
#     if startswith(path, "http://") || startswith(path, "https://")
#         error("Writing to http(s):// is not supported")
#     elseif startswith(path, "s3://")
#         path = Banyan.getpath(path)
#         # NOTE: We expect that the ParallelCluster instance was set up
#         # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
#     else
#         # Prepend "efs/" for local paths
#         path = Banyan.getpath(path)
#     end

#     # Write files for this partition
#     partition_idx = Banyan.get_partition_idx(batch_idx, nbatches, comm)
#     num_partitions = Banyan.get_npartitions(nbatches, comm)
#     for slice_idx in 1:length(part)
#         fpath = "part_$(sortablestring(partition_idx, num_partitions))_slice_$slice_idx"
#         save(fpath, part[i])
#     end

#     # TODO: Is a Barrier needed here?
# end