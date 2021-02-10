using MPI

function split_nothing(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)::Nothing
    nothing
end

function merge_nothing(
    src,
    dst,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    src
end

function worker_idx_and_nworkers(comm::MPI.Comm)
    (MPI.Comm_rank(comm) + 1, MPI.Comm_size(comm))
end

function split_len(src_len::Int64, idx::Int64, npartitions::Int64)
    if npartitions > 1
        dst_len = Int64(cld(src_len, npartitions))
        dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
        dst_end = min(idx * dst_len, src_len)
        (dst_start, dst_end)
    else
        (1, src_len)
    end
end

function split_and_get_len(src_len::Int64, idx::Int64, npartitions::Int64)
    dst_len = split_len(src_len, idx, npartitions)
    length(dst_len[1]:dst_len[2])
end

function split_array(src::Array, idx::Int64, npartitions::Int64, dim::Int8)
    if npartitions > 1
        src_len = size(src, dim)
        dst_start, dst_end = split_len(src_len, idx, npartitions)
        selectdim(src, dim, dst_start:dst_end)
    else
        src
    end
end

function SplitBlockNone(args...) split_nothing(args...) end
function SplitBlockExecutor(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    if isnothing(src)
        nothing
    else
        worker_idx, nworkers = worker_idx_and_nworkers(comm)
        dim = params["dim"]

        dst = split_array(src, worker_idx, nworkers, dim)
        dst = split_array(dst, batch_idx, nbatches, dim)
        dst
    end
end

function MergeBlockBalancedNone(args...) merge_nothing(args...) end
function MergeBlockBalancedExecutor(args...) merge_nothing(args...) end
function MergeBlockUnbalancedNone(args...) merge_nothing(args...) end
function MergeBlockUnbalancedExecutor(args...) merge_nothing(args...) end

function from_jl_value(val::Dict)
    # parse(include_string(Main, "Int64"), "1000000")
    if "banyan_type" in keys(val)
        if val["banyan_type"] == "value"
            include_string(Main, val["contents"])
        else
            parse(include_string(Main, val["banyan_type"]), val["contents"])
        end
    else
        Dict(from_jl_value(k) => from_jl_value(v) for (k, v) in val)
    end
end
function from_jl_value(val::Vector)
    return [from_jl_value(e) for e in val]
end
function from_jl_value(val::Any) val end

function SplitDivValue(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    worker_idx, nworkers = worker_idx_and_nworkers(comm)
    dst_len = split_and_get_len(from_jl_value(loc_parameters["value"]), worker_idx, nworkers)
    dst_len = split_and_get_len(dst_len, batch_idx, nbatches)
    dst_len
end

function SplitDivExecutor(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    worker_idx, nworkers = worker_idx_and_nworkers(comm)
    dst_len = split_and_get_len(src, worker_idx, nworkers)
    dst_len = split_and_get_len(dst_len, batch_idx, nbatches)
    dst_len
end

function SplitReplicateNone(args...) split_nothing(args...) end

function SplitReplicateValue(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    from_jl_value(loc_parameters["value"])
end

function SplitReplicateExecutor(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_parameters,
)
    src
end

function MergeDivValue(args...) merge_nothing(args...) end
function MergeDivExecutor(args...) merge_nothing(args...) end
function MergeReplicateNone(args...) merge_nothing(args...) end
function MergeReplicateValue(args...) merge_nothing(args...) end
function MergeReplicateExecutor(args...) merge_nothing(args...) end
