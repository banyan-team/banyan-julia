using MPI

SPLIT = Dict{String,Dict}()
MERGE = Dict{String,Dict}()
CAST = Dict{String,Dict}()

function return_nothing(
    src,
    dst,
    params,
    batch_idx,
    nbatches,
    comm,
    loc_parameters,
)
    dst[] = nothing
end

worker_idx_and_nworkers(comm) = (MPI.Comm_rank(comm) + 1, MPI.Comm_size(comm))

function split_array(src, dst, idx, npartitions, dim)
    src_len = size(src[], dim)
    dst_len = cld(src_len, npartitions)
    dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
    dst_end = min(idx * dst_len, src_len)
    dst[] = selectdim(src[], dim, dst_start:dst_end)
end

# TODO: Make implementations for None read/write from/to disk

SPLIT["BlockBalanced"] = Dict()
MERGE["BlockBalanced"] = Dict()

SPLIT["BlockBalanced"]["None"] = return_nothing

MERGE["BlockBalanced"]["None"] = return_nothing

SPLIT["BlockBalanced"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        if isnothing(src[])
            dst[] = nothing
        else
            worker_idx, nworkers = worker_idx_and_nworkers(comm)
            dim = params["dim"]

            split_array(src, dst, worker_idx, nworkers, dim)
            split_array(src, dst, batch_idx, nbatches, dim)
        end
    end

MERGE["BlockBalanced"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        # TODO: Implement this

        # if batch_idx == 1
        #     src[] = dst[]
        # else
        #     append!()
        # if isnothing(src[])
        #     src[] = nothing
        # else
        #     worker_idx, nworkers = worker_idx_and_nworkers(comm)
        #     dim = params["dim"]

        #     split_array(src, dst, worker_idx, nworkers, dim)
        #     split_array(src, dst, batch_idx, nbatches, dim)
        # end
    end

SPLIT["BlockUnbalanced"] = Dict()
MERGE["BlockUnbalanced"] = Dict()

SPLIT["BlockUnbalanced"]["None"] = return_nothing

MERGE["BlockUnbalanced"]["None"] = return_nothing

SPLIT["BlockUnbalanced"]["Executor"] = SPLIT["BlockBalanced"]["Executor"]

MERGE["BlockUnbalanced"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        # TODO: Implement this

        # if batch_idx == 1
        #     src[] = dst[]
        # else
        #     append!()
        # if isnothing(src[])
        #     src[] = nothing
        # else
        #     worker_idx, nworkers = worker_idx_and_nworkers(comm)
        #     dim = params["dim"]

        #     split_array(src, dst, worker_idx, nworkers, dim)
        #     split_array(src, dst, batch_idx, nbatches, dim)
        # end
    end

# # using HDF5  # Block-HDF5
# # using DataFrames
# # using DataFramesMeta
# # using Arrow

# #####################
# # WRAPPER FUNCTIONS #
# #####################

# function split(pt_name::String, type::String, args...)
#     SPLIT_IMPL[pt_name][type](args)
# end

# function merge(pt_name::String, type::String, args...)
#     MERGE_IMPL[pt_name][type](args)
# end


# ##################
# # BASE FUNCTIONS #
# ##################

# default_batches_func = function(
#     src, part, parameters, idx, nbatches
# )
# end

# default_workers_func = function(
#     src, part, parameters, comm
# )
# end

# default_lt_func = function(
#     src, part, parameters, idx, nbatches, comm, lt_params
# )
# end


# ###################
# # SPLIT_IMPL FUNCTIONS #
# ###################

# # PT Name --> LT_Name/Workers/Batches --> Implementation
# SPLIT_IMPL = Dict{String, Dict}()

# SPLIT_IMPL["Div"] = Dict()

# SPLIT_IMPL["Div"]["Batches"] = function(
#     src, part, split_params, idx, nbatches
# )
#     part[] = fld(src[], nbatches)
# end

# SPLIT_IMPL["Div"]["Workers"] = function(
#     src, part, split_params, comm
# )
#     part[] = fld(src, MPI.Comm_size(comm))
# end

# SPLIT_IMPL["Div"]["None"] = function(
#     src, part, split_params, idx, nbatches, comm, lt_params
# )
#     part[] = flt(split_params[1], nbatches)
# end

# SPLIT_IMPL["Replicate"] = Dict()

# SPLIT_IMPL["Replicate"]["Batches"] = function(
#     src, part, split_params, idx, nbatches
# )
#     part[] = src[]
# end

# SPLIT_IMPL["Replicate"]["Workers"] = function(
#     src, part, split_params, comm
# )
#     part[] = src[]
# end

# SPLIT_IMPL["Replicate"]["Client"] = function(
#     src, part, split_params, idx, nbatches, comm, lt_params
# )
#     # TODO: Ensure that global comm gets passed as parameter

#     # Main worker sends scatter request on gather queue
#     #   and waits for response on scatter queue
#     value_id = pt.split_params[1]
#     v = nothing
#     if MPI.Comm_rank(comm) == 0
#         send_scatter_request(value_id)

#         m = nothing
#         while (isnothing(m))
#             m = sqs_receive_message(get_scatter_queue())
#         end
#         v = JSON.parse(m[:message])["value"]
#         # TODO: This step is not needed here and
#         #   on client side, if primitive type
#         v = deserialize(IOBuffer(convert(Array{Uint8}, v,)))
#         sqs_delete_message(get_scatter_queue(), m)
#     end

#     part[] = MPI.bcast(v, 0, comm)

# end

# SPLIT_IMPL["Replicate"]["Value"] = function(
#     src, part, split_params, idx, nbatches, comm, lt_params
# )
#     part[] = lt_params[1]
# end

# SPLIT_IMPL["Block"] = Dict()

# SPLIT_IMPL["Block"]["Batches"] = function(
#     src, part, split_params, idx, nbatches
# )

#     if src[] == nothing
#         part[] = nothing
#     else
#         dim = split_params[1]
#         partition_length = cld(size(src[], dim), nbatches)

#         partition_length = cld(size(src[], dim), nbatches)

#         first_idx = min(1 + idx * partition_length, size(src[], dim) + 1)
#         last_idx = min((idx + 1) * partition_length, size(src[], dim))

#         # part is a view into src
#         part[] = selectdim(src[], dim, first_idx:last_idx)
#     end
# endIT_IMPL["Block"]["Workers"] = function(
#     src, part, split_params, idx, nbatches, comm
# )

#     if src[] == nothing
#         part[] = nothing
#     else
#         dim = pt.split_params[1]
#         partition_length = cld(size(src[], dim), nbatches)

#         # TODO: Scatter or Scatter!
#         # TODO: Or should this be idx == 0? replace 0 below with idx then
#         # if MPI.Comm_rank(comm) == 0
#         #     Scatter!(src, nothing, partition_length, 0, comm)
#         # else
#         #     Scatter!(nothing, buf, partition_length, 0, comm)        
#         # end
#         part[] = Scatter(src[], partition_length, 0, comm)
#     end

# end

# SPLIT_IMPL["Block"]["None"] = default_lt_func

# SPLIT_IMPL["Block"]["HDF5"] = function(
#     src, part, split_params, idx, nbatches, comm, lt_params
# )
#     # TODO: Implement this

#     dim = split_params[1]
#     filename = lt_params[2]
#     path = lt_params[3]

#     h5open(filename, comm) do f
#         dset = read(f, path, dxpl_mpio=HDF5.H5FD_MPIO_COLLECTIVE)
#         partition_length = cld(size(dset, dim), nbatches) # TODO: is this nbatches? or nbatches * nworkers?
#         first_idx = min(1 + idx * partition_length, size(dset, dim) + 1)
#         last_idx = min((idx + 1) * partition_length, size(dset, dim))
#         part[] = selectdim(dset, dim, first_idx:last_idx)
#     end

#     MPI.Barrier(comm)

#     # HDF5.h5_close() # TODO: not sure 
# end

# SPLIT_IMPL["Stencil"] = Dict()

# SPLIT_IMPL["Stencil"]["Batches"] = function (
#     src, part, split_params, idx, nbatches
# )
#     # dim = split_params[1]
#     # size = split_params[2]
#     # stride = split_params[3]
#     # @assert length(size) = len(stride)

#     # num_blocks = cld(cld(size(src, dim) - size, stride), nbatches)
#     # first_idx = 1 + idx * stride * num_blocks
#     # last_idx = min(1 + idx * stride * num_blocks + size, size(src, dim))

#     # part = selectdim(src, dim, first_idx:last_idx)

#     dim = split_params[1]
#     left_overlap = split_params[2]
#     right_overlap = split_params[3]


#     partition_length = cld(size(src[], dim), nbatches)

#     first_idx = max(
#                     min(1 + idx * partition_length, size(src[], dim) + 1) - left_overlap,
#                     0
#                 )
#     last_idx = min(
#                     min((idx + 1) * partition_length, size(src[], dim)) + right_overlap,
#                     size(src[], dim)
#                 )

#     part = selectdim(src, dim, first_idx:last_idx)
# end

# SPLIT_IMPL["Stencil"]["Workers"] = function (
#     src, part, split_params, idx, nbatches, comm
# )
#     src, part, split_params, idx, nbatches, comm
# )
#     # TODO: Implement this




# end

# SPLIT_IMPL["Stencil"]["None"] = default_lt_func
# SPLIT_IMPL["Bucket"] = Dict()

# SPLIT_IMPL["Bucket"]["Batches"] = function(
#     src, part, split_params, idx, nbatches
# )
#     # src[] is a Dataframe

#     hash_col = split_params[1]
#     part[] = src[][mod(hash(src[][hash_col]), nbatches) == idx, :]

#     # DT[in([1,4]).(DT.ID), :]
#     # df[df[:A] % 2 .== 0, :]
#     # TODO: does this require a merge

# end

# SPLIT_IMPL["Bucket"]["Workers"] = function()
# end

# SPLIT_IMPL["Bucket"]["None"] = default_lt_func

# SPLIT_IMPL["Bucket"]["CSV"] = function()
# end

# SPLIT_IMPL["Bucket"]["Arrow"] = function(
#     src, part, split_params, idx, nbatches, comm, lt_params
# )

#     hash_col = split_params[1]
#     filename = lt_params[1]

#     # Read from source

#     table = Arrow.Table(filename)
#     meta = Arrow.getmetadata(table)
#     df = DataFrame(table)
#     num_rows = size(df, 1) # TODO: or get num rows from meta?
#     partition_length = cld(size(src[], dim), MPI.Comm_size(comm))
#     first_idx = min(1 + idx * partition_length, num_rows + 1)
#     last_idx = min((idx + 1) * partition_length, num_rows)
#     part[] = df[first_idx:last_idx, :]

#     # Shuffle across workers

#     # TODO: vv

#     # part[] = selectdim(src[], dim, first_idx:last_idx)
#     # src[][mod(hash(src[][hash_col]), nbatches) == idx, :]



#     if nbatches > 1
#         # TODO: Implement this
#     end
# end


# ###################
# # MERGE_IMPL FUNCTIONS #
# ###################

# # PT Name --> LT_Name/Workers/Batches --> Implementation
# MERGE_IMPL = Dict{String, Dict}()

# MERGE_IMPL["Div"] = Dict()

# MERGE_IMPL["Div"]["Workers"] = default_workers_func

# MERGE_IMPL["Div"]["Batches"] = default_batches_func

# MERGE_IMPL["Div"]["None"] = default_lt_func

# MERGE_IMPL["Replicate"] = Dict()

# MERGE_IMPL["Replicate"]["Workers"] = function()
# end

# MERGE_IMPL["Replicate"]["Workers"] = function()
# end

# MERGE_IMPL["Replicate"]["Client"] = function (
#     src, part, merge_params, idx, nbatches, comm, lt_params
# )
#     # TODO: Make sure global comm is passed as parameter

#     global comm

#     value_id = merge_params[1]
#     if MPI.Comm_rank(comm) == 0
#         buf = IOBuffer()
#         serialize(buf, part)
#         value = take!(buf)
#         send_gather(value_id, value)
#     end
# end

# MERGE_IMPL["Replicate"]["Value"] = default_lt_func

# MERGE_IMPL["Block"] = Dict()

# MERGE_IMPL["Block"]["Batches"] = default_batches_func

# MERGE_IMPL["Block"]["Workers"] = function(
#     src, part, merge_params, comm
# )
#     if src[] == nothing
#         println("src is nothing in merge block workers")
#         # TODO: Allocate space and then gather
#     else
#         # TODO: Allgather or Allgather!
#         src[] = Allgather(part[], comm)
#     end
# end

# MERGE_IMPL["Block"]["None"] = default_lt_func

# MERGE_IMPL["Block"]["HDF5"] = function(
#     src, part, splitting_parameters, idx, nbatches, comm, lt_params
# )
#     # TODO: Implement this

#     dim = split_params[1]

#     dim = split_params[1]
#     filename = lt_params[2]
#     path = lt_params[3]

#     h5open(filename, "w", comm) do f
#         # TODO: vv
#         # dset = read(f, path, dxpl_mpio=HDF5.H5FD_MPIO_COLLECTIVE)
#         # dset = create_dataset(ff, "/data", datatype(eltype(A)), dataspace(dims))? or nbatches * nworkers?
#         first_idx = min(1 + idx * partition_length, size(dset, dim) + 1)
#         last_idx = min((idx + 1) * partition_length, size(dset, dim))

#         #selectdim(dset, dim, first_idx:last_idx) = part[]
#         temp = Ref(selectdim(dset, dim, first_idx:last_idx))
#         temp[] = part[]

#     end
# end

# MERGE_IMPL["Stencil"] = Dict()

# MERGE_IMPL["Stencil"]["Batches"] = default_batches_func

# MERGE_IMPL["Stencil"]["Workers"] = function (
#     src, part, merge_params, comm
# )
#     # TODO: Implement this

#     if src == nothing
#         # TODO: Implement this case
#     else
#         dim = split_params[1]
#     else
#         dim = split_params[1]
#         left_overlap = split_params[2]
#         right_overlap = split_params[3]

#         part[] = selectdim(part[], dim, (1 + left_overlap):(size(part[], dim) - right_overlap))
#         # TODO: Allgather or Allgather!
#         src[] = Allgather(part[], comm)
#     end
# MERGE_IMPL["Stencil"]["None"] = default_lt_func

# MERGE_IMPL["Bucket"] = Dict()

# MERGE_IMPL["Bucket"]["Batches"] = function()
# end

# MERGE_IMPL["Bucket"]["Workers"] = function()
# end

# MERGE_IMPL["Bucket"]["None"] = function()
# end

# MERGE_IMPL["Bucket"]["CSV"] = function()
# end

# MERGE_IMPL["Bucket"]["Parquet"] = function()
# end


# ##################
# # CAST FUNCTIONS #
# ##################

# # MERGE_IMPL Name --> Split Name --> Implementation
# CAST = Dict{String, Dict}()
