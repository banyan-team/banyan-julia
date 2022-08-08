Arrow_Table_retry = retry(Arrow.Table; delays=Base.ExponentialBackOff(; n=5))
load_retry = retry(load; delays=Base.ExponentialBackOff(; n=5))

function ReadBlockImageHelper(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    meta_path::String,
    nimages::Int64,
    datasize,
    add_channelview::Int64
)
    # path = Banyan.getpath(loc_params["path"]) ? isa(loc_params["path"], String) : path
    # ndims = loc_params["ndims"]
    # nbytes = loc_params["nbytes"]
    # dataeltype = loc_params["eltype"]
    # file_extension = "." * loc_params["format"]

    # files is either a list of file paths or a serialized tuple containing
    # information to construct a generator
    meta_table = Arrow_Table_retry(meta_path)

    # Identify the range of indices of files for the batch currently
    # being processed by this worker
    filerange = Banyan.split_len(nimages, batch_idx, nbatches, comm)
    files_sub = meta_table.path[filerange]

    part_size = (length(files_sub), (datasize)[2:end]...)
    elty = Banyan.type_from_str(loc_params["eltype"])
    images = Base.Array{elty}(undef, part_size)
    # TODO: Make it so that the Arrow file only contains the paths and the local paths are computed here
    for (i, f) in enumerate(files_sub)
        filepath = Banyan.getpath(f)
        image = load_retry(filepath)
        if add_channelview == 1
            image = ImageCore.channelview(image)
            images[i, :, :, :] = image
        else
            images[i, :, :] = image
        end
    end
    images
end

ReadBlockImage(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = ReadBlockImageHelper(
    src,
    params,
    batch_idx,
    nbatches,
    comm,
    loc_name,
    loc_params,
    loc_params["meta_path"]::String,
    parse(Int64, loc_params["nimages"]),
    Banyan.size_from_str(loc_params["size"]),
    parse(Int64, loc_params["add_channelview"])
)

# function WriteImage(
#     src,
#     part,
#     params,
#     batch_idx::Int64,
#     nbatches::Int64,
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