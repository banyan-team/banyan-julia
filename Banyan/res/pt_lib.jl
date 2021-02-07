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
MERGE["BlockBalanced"]["Executor"] = merge_nothing
MERGE["BlockUnbalanced"]["None"] = merge_nothing
MERGE["BlockUnbalanced"]["Executor"] = merge_nothing

SPLIT["Div"] = Dict()
MERGE["Div"] = Dict()
SPLIT["Replicate"] = Dict()
MERGE["Replicate"] = Dict()

from_jl_value(val) =
    if val isa Dict
        if "banyan_type" in keys(val)
            if val["banyan_type"] == "value"
                eval(Meta.parse(val["contents"]))
            else
                parse(eval(Meta.parse(val["banyan_type"])), val["contents"])
            end
        else
            Dict(from_jl_value(k) => from_jl_value(v) for (k, v) in val)
        end
    elseif val isa Vector
        [from_jl_value(e) for e in val]
    else
        val
    end

SPLIT["Div"]["Value"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        worker_idx, nworkers = worker_idx_and_nworkers(comm)
        dst_len = split_and_get_len(from_jl_value(loc_parameters["value"]), worker_idx, nworkers)
        dst_len = split_and_get_len(dst_len, batch_idx, nbatches)
        dst[] = dst_len
    end

SPLIT["Div"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        worker_idx, nworkers = worker_idx_and_nworkers(comm)
        dst_len = split_and_get_len(src[], worker_idx, nworkers)
        dst_len = split_and_get_len(dst_len, batch_idx, nbatches)
        dst[] = dst_len
    end

SPLIT["Replicate"]["None"] = split_nothing

SPLIT["Replicate"]["Value"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        dst[] = from_jl_value(loc_parameters["value"])
    end

SPLIT["Replicate"]["Executor"] =
    function (src, dst, params, batch_idx, nbatches, comm, loc_parameters)
        dst[] = src[]
    end

MERGE["Div"]["Value"] = merge_nothing
MERGE["Div"]["Executor"] = merge_nothing
MERGE["Replicate"]["None"] = merge_nothing
MERGE["Replicate"]["Value"] = merge_nothing
MERGE["Replicate"]["Executor"] = merge_nothing
