function ReadBlockImage(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    # path = Banyan.getpath(loc_params["path"]) ? isa(loc_params["path"], String) : path
    files = loc_params["files"]
    ndims = loc_params["ndims"]
    nbytes = loc_params["nbytes"]
    nimages = loc_params["nimages"]
    dataeltype = loc_params["eltype"]
    file_extension = "." * loc_params["format"]

    # files is either a list of file paths or a serialized generator
    if !isa(files, Base.Array)
        println("FILES IN PF: ", files)
        files = Banyan.from_jl_value_contents(files)
        for f in Base.collect(files)
            println(f)
        end
    end

    # Identify the range of indices of files for the batch currently
    # being processed by this worker
    filerange = Banyan.split_len(nimages, batch_idx, nbatches, comm)

    # if isa(files, Base.Generator)
    #     files_sub = Iterators.take(Iterators.drop(files, filerange.start - 1), filerange.stop - filerange.start + 1)
    # else
    #     files_sub = view(files, filerange)
    # end

    images = []
    for f in files  #_sub
        filepath = Banyan.getpath(f)
        image = load(filepath)
        push!(images, reshape(image, (1, size(image)...)))
    end
    images = cat(images..., dims=1)
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