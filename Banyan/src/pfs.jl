# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pt_lib.jl` should have a corresponding
# `pt_lib_info.json` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

global parents = IdDict()

function set_parent(child, parent)
    global parents
    parents[child] = parent
end

function get_parent(child)
    global parents
    get(parents, child, nothing)
end

function forget_parent(child)
    global parents
    delete!(parents, child)
end

function forget_parents()
    global parents
    empty!(parents)
end

###################################
# Splitting and merging functions #
###################################

ReturnNull(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

ReturnNull(
    src,
    part,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    src = nothing
    src
end

format_available_memory() =
    format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

sortablestring(val, maxval) = _sortablestring(string(val), string(maxval))
function _sortablestring(val::String, maxval::String)
    s = val
    maxs = maxval
    res = Base.fill('0', length(maxs))
    res[length(res)-length(s)+1:length(res)] .= Base.collect(s)
    join(res)
end

splitting_divisions = IdDict{Any,Any}()

function get_splitting_divisions()
    global splitting_divisions
    splitting_divisions
end

ReadGroupHelper(ReadBlockFunc, ShuffleFunc) = begin
    function ReadGroupHelperFunc(
        src,
        params::Dict{String,Any},
        batch_idx::Int64,
        nbatches::Int64,
        comm::MPI.Comm,
        loc_name::String,
        loc_params::Dict{String,Any},
        divisions::Base.Vector{Division{V}},
        key::K,
        rev
    ) where {V,K}
        # TODO: Store filters in parameters of the PT and use them to do
        # partition pruning, avoiding reads that are unnecessary

        # Get information needed to read in the appropriate group
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
        hasdivision = any(isnotempty, partition_divisions)
        firstdivisionidx = findfirst(isnotempty, partition_divisions)
        lastdivisionidx = findlast(isnotempty, partition_divisions)
        firstbatchidx = nothing
        lastbatchidx = nothing

        # Get the divisions that are relevant to this batch by iterating
        # through the divisions in a stride and consolidating the list of divisions
        # for each partition. Then, ensure we use boundedlower=true only for the
        # first batch and boundedupper=true for the last batch.
        curr_partition_divisions::Base.Vector{Base.Vector{Division{V}}} = []
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

        # TODO: Call ReadBlockFunc with nbatches=1 and pass in a function as
        # filtering_op in the params
        # TODO: Pass in function calling SplitGroup with 

        # Read in each batch and shuffle it to get the data for this partition
        read_block_params = deepcopy(params)
        # We need the divisions by worker for both SplitGroup and Shuffle
        params["divisions_by_partition"] = curr_partition_divisions # for SplitGroup
        params["consolidate"] = true # for SplitGroup
        # We can read with balanced = false because it's going to be shuffled and
        # balanced later
        read_block_params["balanced"] = false # for ReadBlock
        read_block_params["filtering_op"] = unfiltered_df -> begin
            # We just pass in 2 and 3 and COMM_WORLD because these parameters
            # don't really matter. We just want to consolidate and get all the data
            # from the partition that actually applies to one of the divisions for this
            # batch.
            SplitGroup(unfiltered_df, params, 1, 3, comm, "Memory", Dict{String,Any}())
        end # for ReadBlock

        # Read in data for this batch
        part = ReadBlockFunc(src, read_block_params, 1, 1, comm, loc_name, loc_params)
        println("After ReadBlock in ReadGroupHelper on get_worker_idx()=$(get_worker_idx()) and batch_idx=$batch_idx with nrow(part)=$(size(part))")

        delete!(params, "divisions_by_partition")
        params["divisions_by_worker"] = curr_partition_divisions # for Shuffle

        # Shuffle the batch and add it to the set of data for this partition
        part = ShuffleFunc(
            part,
            EMPTY_DICT,
            params,
            comm,
            !hasdivision || batch_idx != firstbatchidx,
            !hasdivision || batch_idx != lastbatchidx,
            false
        )
        println("After Shuffle in ReadGroupHelper on get_worker_idx()=$(get_worker_idx()) and batch_idx=$batch_idx with nrow(part)=$(size(part))")
        delete!(params, "divisions_by_worker")

        # Concatenate together the data for this partition
        res = part

        # If there are no divisions for any of the partitions, then they are all
        # bounded. For a partition to be unbounded on one side, there must be a
        # division(s) for that partition.

        # Store divisions
        if !(res isa Empty)
            splitting_divisions = get_splitting_divisions()
            partition_idx = get_partition_idx(batch_idx, nbatches, comm)
            splitting_divisions[res] =
                (partition_divisions[partition_idx], !hasdivision || partition_idx != firstdivisionidx, !hasdivision || partition_idx != lastdivisionidx)
        end

        println("At end of ReadGroupHelper on get_worker_idx()=$(get_worker_idx()) and batch_idx=$batch_idx with nrow(res)=$(size(res))")

        res
    end
    ReadGroupHelperFunc
end

ReadGroup(ReadGroupHelperFunc) = begin
    function ReadGroupFunc(
        src,
        params::Dict{String,Any},
        batch_idx::Int64,
        nbatches::Int64,
        comm::MPI.Comm,
        loc_name::String,
        loc_params::Dict{String,Any},
    )
        divisions = params["divisions"]
        key = params["key"]
        rev::Bool = get(params, "rev", false) # Passed in ReadBlock
        ReadGroupHelperFunc(
            src,
            params,
            batch_idx,
            nbatches,
            comm,
            loc_name,
            loc_params,
            divisions,
            key,
            rev
        )
    end
    ReadGroupFunc
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
    # to remove all directories at the end of the session.
end

mutable struct PartiallyMerged{T}
    pieces::Vector{Union{Empty,T}}
end

SplitBlock(
    src::Union{Nothing,PartiallyMerged},
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = nothing

SplitBlock(
    src::Empty,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = EMPTY

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
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params;
    store_splitting_divisions = false
) = nothing

SplitGroup(
    src::Empty,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = EMPTY

function MergeHelper(
    src::PartiallyMerged,
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    splitting_divisions,
    key
) where {T}

    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
    # We only need to concatenate partitions if the source is nothing.
    # Because if the source is something, then part must be a view into it
    # and no data movement is needed.

    # Concatenate across batches
    src.pieces[batch_idx] = part
    if batch_idx == nbatches
        delete!(splitting_divisions, part)

        # Concatenate across batches
        to_merge = disallowempty(filter(not_is_empty, src.pieces))
        src = isempty(to_merge) ? EMPTY : merge_on_executor(to_merge, key)
        # src = merge_on_executor(src.pieces; key = key)
        # TODO: Handle case where everything merges to become empty and also ensure WriteHDF5 is correct

        # Concatenate across workers
        nworkers = get_nworkers(comm)
        if nworkers > 1
            src = Consolidate(src, params, EMPTY_DICT, comm)
        end
    end

    # TODO: Handle Consolidate, Merge, WriteHDF5, WriteJuliaArray, WriteCSV/Parquet/Arrow receiving missing

    src
end

function Merge(
    src::Union{Nothing,PartiallyMerged},
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    if batch_idx == 1
        P = typeof(part)
        src = PartiallyMerged{P}(Vector{Union{Empty,P}}(undef, nbatches))
    else
        # Convert the type if needed
        PMT = eltype(src.pieces)
        if !(T <: PMT)
            NT = Union{T,PMT}
            src = PartiallyMerged(convert(Vector{NT}, src.pieces)::Vector{NT})
        end
    end
    MergeHelper(
        src,
        part,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        loc_params,
        get_splitting_divisions(),
        params["key"]
    )
end

Merge(
    src,
    part::Any,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = src

CopyFrom(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = src

CopyFromValue(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = loc_params["value"]

CopyFromClient(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    v::ValueId = loc_params["value_id"]
    received = get_worker_idx(comm) == 1 ? receive_from_client(v) : nothing
    # TODO: Make Replicated not necessarily require it to be replicated _everywhere_
    res = MPI.bcast(received, 0, comm)
    res
end

CopyFromJulia(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    path = getpath(loc_params["path"]::String)
    try
        deserialize_retry(path)
    catch
        # File does not exist
        nothing
    end
end

function CopyTo(
    src,
    part,
    params,
    batch_idx::Int64,
    nbatches::Int64,
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
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
) = begin
    if get_worker_idx(comm) == 1 && batch_idx == 1
        et = @elapsed begin
        send_to_client(loc_params["value_id"], part)
        end
        record_time(:sending_to_client, et)
    end
end

function CopyToJulia(
    src,
    part,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    if get_worker_idx(comm) == 1 && batch_idx == 1
        serialize(getpath(loc_params["path"]), part)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
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

reduce_in_memory(src::Union{Nothing,Empty}, part::T, op::Function) where {T} = part
reduce_in_memory(src::Union{Empty, Nothing}, part::Empty, op::Function) = EMPTY
reduce_in_memory(src, part::Empty, op::Function) = src
reduce_in_memory(src, part::T, op::Function) where {T} = op(src, part)

function ReduceAndCopyToJulia(
    src,
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    @nospecialize(op::Function)
) where {T}
    # Merge reductions from batches
    # TODO: Ensure that we handle reductions that can produce nothing
    println("In ReduceAndCopyToJulia at start with loc_name=$loc_name and part=$part and params=$params")
    src = reduce_in_memory(src, part, op)

    # Merge reductions across workers
    if batch_idx == nbatches
        src = Reduce(src, params, EMPTY_DICT, comm)

        if loc_name != "Memory"
            # We use 1 here so that it is as if we are copying from the head
            # node
            CopyToJulia(src, src, params, 1, 1, comm, loc_name, loc_params)
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
    println("In ReduceAndCopyToJulia at end with loc_name=$loc_name and part=$part")
    src
end

function ReduceAndCopyToJulia(
    src,
    part::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    op = get_op!(params)
    ReduceAndCopyToJulia(
        src,
        part,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        loc_params,
        op
    )
end

ReduceWithKeyAndCopyToJulia = ReduceAndCopyToJulia

Divide(
    src::AbstractRange,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = src[split_len(length(src), batch_idx, nbatches, comm)]

function DivideHelper(
    src::Tuple,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    dim::Int64
)
    # This is for sizes which are tuples.
    part = src
    # part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    newpartdim = length(split_len(part[dim], batch_idx, nbatches, comm))
    indexapply(newpartdim, part, dim)
end

function DivideHelper(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
    dim::Int64
)
    part = src
    # part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    length(split_len(part[dim], batch_idx, nbatches, comm))
end

Divide(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) = DivideHelper(
    src,
    params,
    batch_idx,
    nbatches,
    comm,
    loc_name,
    loc_params,
    params["key"]
)

function DivideFromValue(
    src::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    part = CopyFromValue(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)
end

function DivideFromDisk(
    src::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    part = CopyFromJulia(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)
end

function DivideFromClient(
    src::T,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
) where {T}
    part = CopyFromClient(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)
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
    op = empty_handler(get_op!(src_params))

    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

    if Banyan.INVESTIGATING_LOSING_DATA
        println("In Reduce before MPI.Allreduce with part=$part")
    end

    # Perform reduction
    empty_worker_idx = find_worker_idx_where(part isa Empty; comm=comm)
    part = if empty_worker_idx == -1
        reduce_and_sync_across(op, part, comm=comm)
    else
        gathered = gather_across(part, comm)
        sync_across(
            if is_main_worker(comm)
                let gathered_nonempty = disallowempty(filter(not_is_empty, gathered))
                    isempty(gathered_nonempty) ? EMPTY : Base.reduce(op, gathered_nonempty)
                end
            else
                nothing
            end;
            comm=comm
        )
    end

    if Banyan.INVESTIGATING_LOSING_DATA
        println("In Reduce after MPI.Allreduce with part=$part")
    end
    
    part
end

ReduceWithKey = Reduce

Distribute(part::Nothing, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing

function Distribute(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T}
    # TODO: Determine whether copy is needed
    copy(SplitBlock(part, dst_params, 1, 1, comm, "Memory", EMPTY_DICT))
end

DistributeAndShuffle(part::Nothing, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) = nothing

DistributeAndShuffle(part::T, src_params::Dict{String,Any}, dst_params::Dict{String,Any}, comm::MPI.Comm) where {T} =
    SplitGroup(part, dst_params, 1, 1, comm, "Memory", EMPTY_DICT, store_splitting_divisions = true)