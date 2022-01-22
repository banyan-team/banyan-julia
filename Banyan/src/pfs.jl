# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pt_lib.jl` should have a corresponding
# `pt_lib_info.json` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

###################################
# Splitting and merging functions #
###################################

ReturnNull(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = nothing

ReturnNull(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    src = nothing
    src
end

format_available_memory() =
    format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

function sortablestring(val, maxval)
    s = string(val)
    maxs = string(maxval)
    res = fill('0', length(maxs))
    res[length(res)-length(s)+1:length(res)] .= collect(s)
    join(res)
end

splitting_divisions = IdDict()

function get_splitting_divisions()
    global splitting_divisions
    splitting_divisions
end

ReadGroup(ReadBlock) = begin
    function ReadGroup(
        src,
        params,
        batch_idx::Integer,
        nbatches::Integer,
        comm::MPI.Comm,
        loc_name,
        loc_params,
    )
        # TODO: Store filters in parameters of the PT and use them to do
        # partition pruning, avoiding reads that are unnecessary

        # Get information needed to read in the appropriate group
        divisions = params["divisions"]
        key = params["key"]
        rev = params["rev"] # Passed in ReadBlock
        nworkers = get_nworkers(comm)
        npartitions = nworkers * nbatches
        partition_divisions = get_divisions(divisions, npartitions)

        # TODO: Do some reversing here instead of only doing it later in Shuffle
        # to ensure that sorting in reverse order works correctly

        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any(x->!isempty(x), partition_divisions)
        firstdivisionidx = findfirst(x->!isempty(x), partition_divisions)
        lastdivisionidx = findlast(x->!isempty(x), partition_divisions)
        firstbatchidx = nothing
        lastbatchidx = nothing

        # Get the divisions that are relevant to this batch by iterating
        # through the divisions in a stride and consolidating the list of divisions
        # for each partition. Then, ensure we use boundedlower=true only for the
        # first batch and boundedupper=true for the last batch.
        curr_partition_divisions = []
        for worker_division_idx = 1:nworkers
            for batch_division_idx = 1:nbatches
                # partition_division_idx =
                #     (worker_division_idx - 1) * nbatches + batch_division_idx
                partition_division_idx =
                    get_partition_idx(batch_division_idx, nbatches, worker_division_idx)
                if batch_division_idx == batch_idx
                    # Get the divisions for this partition
                    p_divisions = partition_divisions[partition_division_idx]

                    # We've already used `get_divisions` to get a list of min-max
                    # tuples (we call these tuples "divisions") for each partition
                    # that `ReadGroup` produces. But we only want to read in all
                    # the partitions relevant for this batch. But it is important
                    # then that `curr_partition_divisions` has an element for each
                    # worker. That way, when we call `Shuffle`, it will properly
                    # read data onto each worker that is in the appropriate
                    # partition.
                    push!(
                        curr_partition_divisions,
                        p_divisions,
                    )
                end

                # Find the batches that have the first and last divisions
                if partition_division_idx == firstdivisionidx
                    firstbatchidx = batch_division_idx
                end
                if partition_division_idx == lastdivisionidx
                    lastbatchidx = batch_division_idx
                end
            end
        end

        # Read in each batch and shuffle it to get the data for this partition
        parts = []
        for i = 1:nbatches
            # Read in data for this batch
            part = ReadBlock(src, params, i, nbatches, comm, loc_name, loc_params)

            # Shuffle the batch and add it to the set of data for this partition
            params["divisions_by_worker"] = curr_partition_divisions
            push!(
                parts,
                Shuffle(
                    part,
                    Dict{String,Any}(),
                    params,
                    comm,
                    boundedlower = !hasdivision || batch_idx != firstbatchidx,
                    boundedupper = !hasdivision || batch_idx != lastbatchidx,
                    store_splitting_divisions = false
                ),
            )
            delete!(params, "divisions_by_worker")
        end

        # Concatenate together the data for this partition
        res = merge_on_executor(parts...; key = key)

        # If there are no divisions for any of the partitions, then they are all
        # bounded. For a partition to be unbounded on one side, there must be a
        # division(s) for that partition.

        # Store divisions
        splitting_divisions = get_splitting_divisions()
        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        splitting_divisions[res] =
            (partition_divisions[partition_idx], !hasdivision || partition_idx != firstdivisionidx, !hasdivision || partition_idx != lastdivisionidx)

        res
    end
    ReadGroup
end

function rmdir_on_nfs(actualpath)
    if isdir(actualpath)
        for actualpath_f in readdir(actualpath, join=true)
            if isfile(actualpath_f)
                rm(actualpath_f, force=true)
            end
        end
    end
    # TODO: Also try to remove the directory itself right away although there
    # might still be .nfs files in it. This isn't too much of a problem since we _do_ try
    # to remove all directories at the end of the job.
end

mutable struct PartiallyMerged
    pieces::Vector{Any}
end

SplitBlock(
    src::Union{Nothing,PartiallyMerged},
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

# NOTE: The way we have `partial_merges` requires us to be splitting from
# `nothing` and then merging back. If we are splitting from some value and
# then expecting to merge back in some way then that won't work. If we are
# splitting from a value we assume that we don't have to merge back either
# because we split with a view (so the source was directly mutated) or we
# didn't mutate this value at all. If we are doing in-place mutations where
# we split from some value and then merge back up, then we might have to
# add support for that. Right now, because of the way `SplitBlock`,
# `SplitGroup`, and `Merge` are implemented, we unnecessarily concatenate
# in the case where we are doing things like `setindex!` with a somewhat
# faked mutation.

# src is [] if we are partially merged (because as we iterate over
# batches we take turns between splitting and merging)
SplitGroup(
    src::Union{Nothing,PartiallyMerged},
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
) = nothing

Consolidate(part::Any, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) =
    error("Consolidating $(typeof(part)) not supported")

function Merge(
    src::Union{Nothing,PartiallyMerged},
    part,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    # TODO: Ensure we can merge grouped dataframes if computing them

    splitting_divisions = get_splitting_divisions()

    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
    # We only need to concatenate partitions if the source is nothing.
    # Because if the source is something, then part must be a view into it
    # and no data movement is needed.

    key = params["key"]

    # Concatenate across batches
    if batch_idx == 1
        src = PartiallyMerged(Vector{Any}(undef, nbatches))
    end
    src.pieces[batch_idx] = part
    if batch_idx == nbatches
        delete!(splitting_divisions, part)

        # Concatenate across batches
        src = merge_on_executor(src.pieces...; key = key)

        # Concatenate across workers
        nworkers = get_nworkers(comm)
        if nworkers > 1
            src = Consolidate(src, params, Dict{String,Any}(), comm)
        end
    end

    src
end

Merge(
    src,
    part,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = src

CopyFrom(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = src

CopyFromValue(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = loc_params["value"]

CopyFromClient(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    received = get_worker_idx(comm) == 1 ? receive_from_client(loc_params["value_id"]) : nothing
    # TODO: Make Replicated not necessarily require it to be replicated _everywhere_
    received = MPI.bcast(received, 0, comm)
    received
end

CopyFromJulia(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = isfile(getpath(loc_params["path"])) ? deserialize(path) : nothing

function CopyTo(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    src = part
end

CopyToClient(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) =  if get_partition_idx(batch_idx, nbatches, comm) == 1
    send_to_client(loc_params["value_id"], part)
end

CopyToJulia(
    src,
    part,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = if get_partition_idx(batch_idx, nbatches, comm) == 1
    # # This must be on disk; we don't support Julia serialized objects
    # # as a remote location yet. We will need to first refactor locations
    # # before we add support for that.
    # if isa_gdf(part)
    #     part = nothing
    # end
    serialize(path, part)
end

function get_op!(params::Dict{String,Any})
    op = params["reducer"]
    if params["with_key"]
        key = params["key"]
        if !haskey(params, "reducer_with_key")
            op = op(key)
            reducer_with_key = Dict(key => op)
            params["reducer_with_key"] = reducer_with_key
        else
            reducer_with_key = params["reducer_with_key"]
            if !haskey(reducer_with_key, key)
                op = op(key)
                reducer_with_key[key] = op
            else
                op = reducer_with_key[key]
            end
        end
    end
    op
end

reduce_in_memory(src::Nothing, part::T, op::Function) where {T} = part
reduce_in_memory(src, part::T, op::Function) where {T} = op(src, part)

function ReduceAndCopyToJulia(
    src,
    part::T,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) where {T}
    # Merge reductions from batches
    op = get_op!(params)
    # TODO: Ensure that we handle reductions that can produce nothing
    src = reduce_in_memory(src, part, op)

    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, params, Dict{String,Any}(), comm)

        if loc_name != "Memory"
            # We use 1 here so that it is as if we are copying from the head
            # node
            CopyToJulia(src, src, params, 1, nbatches, comm, loc_name, loc_params)
        end
    end

    # TODO: Ensure we don't have issues where with batched execution we are
    # merging to the thing we are splitting from
    # NOTE: We are probably okay for now because we never split and then
    # re-merge new results to the same variable. We always merge to a new
    # variable. But in the future to be more robust, we may want to store
    # partial merges in a global `IdDict` and then only mutate `src` once we
    # are finished with the last batch and we know we won't be splitting
    # from the value again.
    src
end

ReduceWithKeyAndCopyToJulia = ReduceAndCopyToJulia

function Divide(
    src::Tuple,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    dim = params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    newpartdim = length(split_len(part[dim], batch_idx, nbatches, comm))
    indexapply(_ -> newpartdim, part, index = dim)
end

function Divide(
    src,
    params::Dict{String,Any},
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    dim = params["key"]
    part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    length(split_len(part[dim], batch_idx, nbatches, comm))
end

#####################
# Casting functions #
#####################

function Reduce(
    part::T,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) where {T}
    # Get operator for reduction
    op = get_op!(src_params)

    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

    # Perform reduction
    part = MPI.Allreduce(
        part,
        # sendbuf,
        # (a, b) -> begin
        #     # tobuf(op(frombuf(kind, a), frombuf(kind, b)))[2]
        #     op(a, b)
        # end,
        op,
        comm,
    )
    part
end

ReduceWithKey = Reduce

Banyan.Rebalance(
    part::Any,
    src_params::Dict{String,Any},
    dst_params::Dict{String,Any},
    comm::MPI.Comm
) = error("Rebalancing $(typeof(part)) not supported")

function Distribute(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T}
    # TODO: Determine whether copy is needed
    copy(SplitBlock(part, dst_params, 1, 1, comm, "Memory", Dict{String,Any}()))
end

DistributeAndShuffle(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T} =
    SplitGroup(part, dst_params, 1, 1, comm, "Memory", Dict{String,Any}(), store_splitting_divisions = true)
