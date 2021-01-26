using MPI

SPLIT = Dict{String,Dict}()
MERGE = Dict{String,Dict}()
CAST = Dict{String,Dict}()

function split_nothing(
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

function merge_nothing(
    src,
    dst,
    params,
    batch_idx,
    nbatches,
    comm,
    loc_parameters,
) end

worker_idx_and_nworkers(comm) = (MPI.Comm_rank(comm) + 1, MPI.Comm_size(comm))

function split_len(src_len, idx, npartitions)
    if npartitions > 1
        dst_len = Integer(cld(src_len, npartitions))
        dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
        dst_end = min(idx * dst_len, src_len)
        (dst_start, dst_end)
    else
        (1, src_len)
    end
end

function split_and_get_len(src_len, idx, npartitions)
    dst_len = split_len(src_len, idx, npartitions)
    length(dst_len[1]:dst_len[2])
end

function split_array(src, dst, idx, npartitions, dim)
    if npartitions > 1
        src_len = size(src[], dim)
        dst_start, dst_end = split_len(src_len, idx, npartitions)
        dst[] = selectdim(src[], dim, dst_start:dst_end)
    else
        dst[] = src[]
    end
end

# TODO: Make implementations for None read/write from/to disk

SPLIT["Block"] = Dict()
MERGE["BlockBalanced"] = Dict()
MERGE["BlockUnbalanced"] = Dict()

SPLIT["Block"]["None"] = split_nothing

SPLIT["Block"]["Executor"] =
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

MERGE["BlockBalanced"]["None"] = merge_nothing

# TODO: Implement this
MERGE["BlockBalanced"]["Executor"] = merge_nothing

MERGE["BlockUnbalanced"]["None"] = merge_nothing

# TODO: Implement this
MERGE["BlockUnbalanced"]["Executor"] = merge_nothing

SPLIT["Div"] = Dict()
MERGE["Div"] = Dict()
SPLIT["Replicate"] = Dict()
MERGE["Replicate"] = Dict()

SPLIT["Div"]["Value"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        worker_idx, nworkers = worker_idx_and_nworkers(comm)
        dst_len = split_and_get_len(loc_parameters["value"], worker_idx, nworkers)
        dst_len = split_and_get_len(dst_len, worker_idx, nworkers)
        dst[] = dst_len
    end

SPLIT["Div"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        worker_idx, nworkers = worker_idx_and_nworkers(comm)
        dst_len = split_and_get_len(src[], worker_idx, nworkers)
        dst_len = split_and_get_len(dst_len, worker_idx, nworkers)
        dst[] = dst_len
    end

SPLIT["Replicate"]["Value"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        dst[] = loc_parameters["value"]
    end

SPLIT["Replicate"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        dst[] = src[]
    end

MERGE["Div"]["Value"] = merge_nothing
MERGE["Div"]["Executor"] = merge_nothing
MERGE["Replicate"]["Value"] = merge_nothing
MERGE["Replicate"]["Executor"] = merge_nothing
