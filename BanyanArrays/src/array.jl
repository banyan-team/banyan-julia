function pts_for_replicating(futures::Base.Vector{Future})
    fut, res = futures
    pt(fut, Replicating())
    pt(res, Replicating(), match=fut)
end

function Base.copy(fut::AbstractFuture)
    @nospecialize

    res = Future(from=fut)

    # TODO: Ensure that the CopyTo function can always be used to split anything
    # for which there is no specified PT. But make sure that other splitting
    # functions such as those that split arrays are expecting a name to be
    # provided. If we do that, we can un-comment the below and allow for
    # copying anything.
    partitioned_with(pts_for_replicating, [fut, res], scaled=[fut, res])

    @partitioned fut res begin
        res = Base.copy(fut)
    end

    res
end

function Base.deepcopy(fut::AbstractFuture)
    @nospecialize

    res = Future(from=fut)

    # TODO: Ensure that the CopyTo function can always be used to split anything
    # for which there is no specified PT. But make sure that other splitting
    # functions such as those that split arrays are expecting a name to be
    # provided. If we do that, we can un-comment the below and allow for
    # copying anything  
    partitioned_with(pts_for_replicating, [fut, res], scaled=[fut, res])

    @partitioned fut res begin
        res = Base.deepcopy(fut)
    end

    res
end

# Array type

mutable struct Array{T,N} <: AbstractFuture where {T,N}
    data::Future
    size::Future
    # TODO: Add offset for indexing
    # offset::Future

    Array{T,N}() where {T,N} = new(Future(datatype="Array"), Future())
    Array{T,N}(A::Array{T,N}) where {T,N} = new(Future(datatype="Array"), Future(from=A.size))
    Array{T,N}(data::Future, size::Future) where {T,N} = new(data, size)
end

const Vector{T} = Array{T,1}
const Matrix{T} = Array{T,2}

Base.convert(::Type{Array{T}}, A::AbstractArray{T,N}) where {T,N} = Array{T,N}(Future(A, datatype="Array"), Future(size(A)))
Base.convert(::Type{Array}, arr::AbstractArray{T}) where {T} = convert(Array{T}, arr)
Base.convert(::Type{Vector{T}}, arr::AbstractVector) where {T} = convert(Array{T}, arr)

Banyan.convert(::Type{Future}, A::Array{T,N}) where {T,N} = A.data

# Array sample

function Banyan.sample_axes(A::Base.AbstractArray{T,N})::Base.Vector{Int64} where {T,N} Base.collect(1:ndims(A)) end
function Banyan.sample_keys(A::Base.AbstractArray{T,N})::Base.Vector{Int64} where {T,N} sample_axes(A) end

# `sample_divisions`, `sample_percentile`, and `sample_max_ngroups` should
# work with the `orderinghash` of values in the data they are used on

function Banyan.sample_by_key(A::Base.AbstractArray{T,N}, key::Int64) where {T,N}
    map(first, eachslice(A, dims=key))
end
function Banyan.sample_by_key(A::Base.AbstractVector{T}, key::Int64) where {T}
    A
end

# Array creation

pts_for_blocked_and_replicated(futures::Base.Vector{Future}) =
    pt(futures[1], Blocked(futures[1]) | Replicated())

function Banyan.compute_inplace(A::Array{T,N}) where {T,N}
    partitioned_computation(pts_for_blocked_and_replicated, A, destination=Disk())
end

function pts_for_fill(futures::Base.Vector{Future})
    A, fillingdims, v = futures
    # blocked
    # TODO: Ensure that we are properly creating new PAs
    # We have to create the array in a balanced manner or we have
    # to TODO make some way for PFs for separate futures to
    # communicate with each other and determine how one is grouping
    # so that they both can group
    pt(A, Blocked(A, balanced=true))
    pt(fillingdims, Divided())

    # replicated
    pt(A, fillingdims, v, Replicated())
end

function partitioned_for_fill(A::Future, fillingdims::Future, v::Future)
    # We use `partitioned_with` here to ensure that a sample of A is produced
    # first so that we can use the Blocked PT constructor which depends on A
    # having its sample taken
    partitioned_with(pts_for_fill, [A, fillingdims, v], scaled=[A])

    @partitioned A v fillingdims begin
        A = Base.fill(v, fillingdims)
    end
end

function fill(v, dims::NTuple{N,Integer}) where {N}
    @nospecialize

    fillingdims = Future(source=Size(dims))
    A = Future(datatype="Array")
    A_dims = Future(dims)
    v_sample = copy(v)
    v = Future(v)
    dims = Future(dims)

    # For futures that contains dims or nrows, we initialize them to store the
    # dimensions or # of rows of the whole value. Then if we need to mutate,
    # we reassign to the dimensions or # of rows of the part and assign a
    # reducing PT.

    # NOTE: If a value is being created for the first time in a code region, it
    # is being mutated. The only exception is a value that is already created
    # on the client and is merely being replicated in the code region.

    partitioned_for_fill(A, fillingdims, v)

    Array{typeof(v_sample),N}(A, A_dims)
end

fill(v, dims::Int64...) = fill(v, Tuple(dims))

function pts_for_collect(futures::Base.Vector{Future})
    A, r = futures
    pt(A, Blocked(A, balanced=true))
    pt(r, Divided())
    pt(A, r, Replicated())
end

function partitioned_for_collect(A::Future, r::Future)
    # Define how the data can be partitioned (mentally taking into account
    # data imbalance and grouping)
    partitioned_with(pts_for_collect, [A, r], scaled=[A])

    # Offload the partitioned computation
    @partitioned r A begin A = Base.collect(r) end
end

function collect(r::AbstractRange)
    @nospecialize
    
    # Create output futures
    r_sample = copy(r)
    r = Future(r)
    A = Future(datatype="Array")

    partitioned_for_collect(A, r)

    # Return a Banyan vector as the result
    Vector{eltype(r_sample)}(A, Future((length(r_sample),)))
end

@nospecialize

zeros(::Type{T}, args...; kwargs...) where {T} = fill(zero(T), args...; kwargs...)
zeros(args...; kwargs...) where {T} = zeros(Float64, args...; kwargs...)
ones(::Type{T}, args...; kwargs...) where {T} = fill(one(T), args...; kwargs...)
ones(args...; kwargs...) where {T} = ones(Float64, args...; kwargs...)
trues(args...; kwargs...) where {T} = fill(true, args...; kwargs...)
falses(args...; kwargs...) where {T} = fill(false, args...; kwargs...)

@specialize

# Array properties

Base.ndims(A::Array{T,N}) where {T,N} = ndims(sample(A))
Base.size(A::Array{T,N}) where {T,N} = compute(A.size)
Base.length(V::Array{T,N}) where {T,N} = prod(compute(V.size))
Base.eltype(A::Array{T,N}) where {T,N} = eltype(sample(A))

function pts_for_copying(futures::Base.Vector{Future})
    A::Future, res::Future = futures

    # balanced
    pt(A, Blocked(A, balanced=true))
    pt(res, Balanced(), match=A)

    # unbalanced
    pt(A, Blocked(A, balanced=false, scaled_by_same_as=res))
    pt(res, Unbalanced(A), match=A)
    
    # replicated
    pt(A, res, Replicated())
end

function Base.copy(A::Array{T,N})::Array{T,N} where {T,N}
    res_size = deepcopy(A.size)
    res = Future(datatype="Array")

    partitioned_with(pts_for_copying, [A.data, res], scaled=[A.data, res], keep_same_keys=true)

    @partitioned A res begin
        res = Base.copy(A)
    end

    Array{T,N}(res, res_size)
end

function Base.deepcopy(A::Array{T,N})::Array{T,N} where {T,N}
    res_size = deepcopy(A.size)
    res = Future(datatype="Array")

    partitioned_with(pts_for_copying, [A.data, res], scaled=[A.data, res], keep_same_keys=true)

    @partitioned A res begin
        res = Base.deepcopy(A)
    end

    Array{T,N}(res, res_size)
end

# Array operations

function make_map_res(res_sample::Base.AbstractArray{T,N}, res::Future, res_size::Future)::Array{T,N} where {T,N}
    Array{T,N}(res, res_size)
end

function make_map_res(res_sample::Any, res::Future, res_size::Future)::Future
    res
end

function add_sizes_on_axis(axis::Int64)
    (a, b) -> Banyan.indexapply(+, a, b, axis)
end

function pts_for_map_params(futures::Base.Vector{Future})
    c = futures

    # balanced
    c_first::Future = first(c)
    pt(c_first, Blocked(c_first, balanced=true))
    pt(c[2:end]..., BlockedAlong() & Balanced(), match=c_first, on=["key"])

    # unbalanced
    pt(c_first, Blocked(c_first))
    pt(c[2:end]..., ScaledBySame(c_first), match=c_first)

    # replicated
    pt(c..., Replicated())
end

function pts_for_map(futures::Base.Vector{Future})
    c = futures[1:end-3]
    res, f = futures[end-2:end-1]
    no_replication::Bool = sample(futures[end])

    # balanced
    c_first::Future = first(c)
    pt(c_first, Blocked(c_first, balanced=true))
    pt(c[2:end]..., BlockedAlong() & Balanced(), match=c_first, on=["key"])
    pt(res, BlockedAlong() & Balanced(), match=c_first)

    # unbalanced
    pt(c_first, Blocked(c_first))
    pt(c[2:end]..., ScaledBySame(c_first), match=c_first)
    pt(res, ScaledBySame(c_first), match=c_first)

    # replicated
    if no_replication
        pt(f, Replicated())
    else
        pt(c..., f, Replicated())
        pt(res, Replicated())
    end
end

function Base.map(f, c::Array{<:Any,N}...; force_parallelism=false) where {T,N}
    @nospecialize

    # We shouldn't need to keep sample keys since we are only allowing data
    # to be blocked for now. The sample rate is kept because it might be
    # smaller if this is a column of the result of a join operation.
    # keep_all_sample_keys(res, fut)

    # TODO: Determine whether array operations need to use mutated_from or mutated_to
    # TODO: Instead just make the Array constructor have a code region using
    # the data in a replicated way.

    c_args::Base.Vector{Future} = convert(Base.Vector{Future}, Base.collect(c))
    f = Future(f)

    if force_parallelism
        # If we are forcing parallelism, we have an empty code region to
        # allow for copying from sources like client side and then casting
        # from replicated partitioning to distributed partitioning

        for c_arg in c_args
            mutated(c_arg)
        end

        partitioned_with(pts_for_map_params, c_args, scaled=c_args)

        @partitioned c begin end
    end

    res_size = deepcopy(first(c).size)
    res = Future(datatype="Array")

    partitioned_with(pts_for_map, [c_args..., res, f, Future(force_parallelism)], scaled=c_args)

    @partitioned f c res begin
        res = Base.map(f, c...)
    end

    make_map_res(sample(res), res, res_size)
end

function pts_for_mapslices(futures::Base.Vector{Future})
    f, A, res_size, res, dims = futures

    dims_sample = sample(dims)
    dims_sample_isa_colon = dims_sample isa Colon
    dims_sample_res = dims_sample_isa_colon ? Int64[] : Base.collect(tuple(dims_sample))

    # Blocked PTs along dimensions _not_ being mapped along
    bpt = [bpt for bpt in Blocked(A) if !(dims_sample_isa_colon) && !(bpt.parameters["key"] in dims_sample_res)]

    if !isempty(bpt)
        # balanced
        pt(A, bpt & Balanced())
        pt(res, BlockedAlong() & Balanced(), match=A, on="key")

        # unbalanced
        pt(A, bpt & Unbalanced(res))
        pt(res, Unbalanced(A), match=A)
    end

    # replicated
    # TODO: Determine why this MatchOn constraint is not propagating
    pt(res_size, ReducingWithKey(add_sizes_on_axis), match=A, on="key")
    pt(A, res, res_size, f, dims, Replicated())
end

function _mapslices(f::Future, A::Future, res_size::Future, res::Future, dims::Future)
    partitioned_with(pts_for_mapslices, [f, A, res_size, res, dims], scaled=[A, res])

    @partitioned f A dims res res_size begin
        # We return nothing because `mapslices` doesn't work properly for
        # empty data
        res = isempty(A) ? EMPTY : Base.mapslices(f, A, dims=dims)
        res_size = isempty(A) ? EMPTY : Base.size(res)
    end

    make_map_res(sample(res), res, res_size)
end

function Base.mapslices(f, A::Array{T,N}; dims) where {T,N}
    @nospecialize

    if isempty(dims) return map(f, A) end

    f = Future(f)
    res_size = Future()
    # TODO: Ensure that this usage of Any is correct here and elsewhere
    res = Future(datatype="Array")
    dims = Future(dims)

    _mapslices(f, A.data, res_size, res, dims)
end

function pts_for_getindex(futures::Base.Vector{Future})
    A, indices, res_size, res = futures

    A_sample = sample(A)
    indices_sample = sample(indices)
    single_colon = length(indices_sample) == 1 && indices_sample[1] isa Colon
    allowed_splitting_dims::Base.Vector{Int64} = if single_colon
        # Return the last dimension that isn't length-1
        # NOTE: We assume here that samples are taken on the first dimension.
        # The main reason for not just returning `ndims(A_sample)` is that if we
        # have an array of images that are passed into `mapslices(v->[v])` then we
        # don't want to column-partition that when there is a single column.
        A_sample_ndims = ndims(A_sample)
        last_non_one_dim = A_sample_ndims
        if !Banyan.INVESTIGATING_DIFFERENT_PARTITIONING_DIMS
            # Iterate from the dimension before the last (the one we have chosen right now)
            for i in (A_sample_ndims-1):-1:1
                # Check if the currently selected dimension is 1, if so, change
                # it to this one
                if size(A_sample, i+1) == 1
                    last_non_one_dim = i
                end
            end
        end
        Int64[last_non_one_dim]
    elseif length(indices_sample) == ndims(A_sample)
        Int64[i for i in 1:ndims(A_sample) if indices_sample[i] isa Colon]
    else
        Int64[]
    end

    # Blocked PTs along dimensions _not_ being mapped along
    bpt = PartitionType[bpt for bpt in Blocked(A) if (bpt.parameters["key"])::Int64 in allowed_splitting_dims]

    if !isempty(bpt)
        # balanced
        pt(A, bpt & Balanced())
        if single_colon
            pt(res, BlockedAlong(1) & Balanced())
        else
            pt(res, BlockedAlong() & Balanced(), match=A, on="key")
        end

        # unbalanced
        pt(A, bpt & Unbalanced(res))
        pt(res, Unbalanced(A), match=A)

        # # Keep the same kind of size
        # pt(A_size, Replicating())
        # pt(res_size, PartitionType(), match=A_size)
        # TODO: See if `quote` is no longer needed
        pt(res_size, ReducingWithKey(add_sizes_on_axis), match=res, on="key")
    end

    pt(A, res, res_size, indices, Replicated())
end

function _getindex(A::Future, indices::Future, res_size::Future, res::Future)
    partitioned_with(pts_for_getindex, [A, indices, res_size, res], scaled=[A, res])

    # TODO: Add back in A_size and try to use it with mutation= in the `Future`
    # constructor to avoid having to do a reduction to compute size
    @partitioned A indices res res_size begin
        res = Base.getindex(A, indices...)
        # res_size = BanyanArrays.getindex_size(A_size, indices...)
        if res isa AbstractArray
            res_size = size(res)
        end
    end

    res_sample = sample(res)
    make_map_res(res_sample, res, res_size)
end

function Base.getindex(A::Array{T,N}, indices...) where {T,N}
    @nospecialize

    # If we are doing linear indexing, then the data can only be split on the
    # last dimension because of the column-major ordering
    for i in indices
        (i isa Colon || i isa Integer || i isa Vector) || error("Expected indices to be either integers, vectors of integers, or colons")
    end

    indices = Future(indices)
    res_size = Future()
    res = Future(datatype="Array")

    _getindex(A.data, indices, res_size, res)
end

# TODO: Implement reduce and sortslices

function pts_for_reduce(futures::Base.Vector{Future})
    op, A, res_size, res, dims, kwargs = futures
    op_sample = sample(op)
    dims_sample = sample(dims)
    dims_sample_isa_colon = dims_sample isa Colon
    dims_sample_res = dims_sample isa Colon ? Int64[] : Base.collect(tuple(dims_sample))
    for bpt in Blocked(A)
        pt(A, bpt)
        if dims_sample_isa_colon || bpt.parameters["key"] in dims_sample_res
            # NOTE: Be careful about trying to serialize things that would
            # require serializing the whole Banyan module. For example, if
            # this where Reducing(op) or if we tried Future(op) where op
            # could refer to a + function overloaded by BanyanArrays.
            pt(res, Reducing(op_sample))
        else
            pt(res, bpt.balanced ? Balanced() : Unbalanced(A), match=A)
        end
    end
    pt(res_size, ReducingWithKey(add_sizes_on_axis), match=A, on="key")
    # TODO: Allow replication
    if !is_debug_on()
        pt(A, res, res_size, dims, kwargs, op, Replicated())
    else
        pt(dims, kwargs, op, Replicated())
    end
end

function _reduce(op::Future, A::Future, res_size::Future, res::Future, dims::Future, kwargs::Future)
    # TODO: Don't mark res as scaled if dims includes 1. This is because we sample
    # by randomly selecting rows (dim = 1) and so when we reduce then if we annotate the
    # result as scaled, the result's memory usage will be greater than it actually is. This could
    # cause problems by making it impossible to replicate and return the data to the client side.
    partitioned_with(pts_for_reduce, [op, A, res_size, res, dims, kwargs], scaled=[A, res])
    # TODO: Duplicate annotations to handle the balanced and unbalanced cases
    # seperately
    # TODO: Have a better API where duplicating to handle balanced and unbalanced
    # isn't needed
    # TODO: Cascaade MatchOn constraints
    # TODO: Ensure that MatchOn value is being discovered

    @partitioned op A dims kwargs res res_size begin
        if isempty(A)
            res = EMPTY
            res_size = EMPTY
        else
            res = Base.reduce(op, A; dims=dims, kwargs...)
            if res isa AbstractArray
                res_size = Base.size(res)
            else
                res_size = EMPTY
            end
        end
    end

    res_sample = sample(res)
    make_map_res(res_sample, res, res_size)
end

function Base.reduce(op, A::Array{T,N}; dims=:, kwargs...) where {T,N}
    @nospecialize

    if haskey(kwargs, :init) throw(ArgumentError("Reducing with an initial value is not currently supported")) end

    op = Future(op)
    res_size = Future()
    res = dims isa Colon ? Future() : Future(datatype="Array")
    dims = Future(dims)
    kwargs = Future(kwargs)

    _reduce(op, A.data, res_size, res, dims, kwargs)
end

function pts_for_sortslices(futures::Base.Vector{Future})
    A::Future, res::Future, dims::Future, kwargs::Future = futures

    # Some mapping computation might produce an AbstractArray that isn't
    # a Base.Array and then we would have to change this type annotation
    A_sample::SampleForGrouping{Base.Array{T,N},Int64} = sample_for_grouping(A)
    isreversed = get(sample(kwargs), :rev, false)::Bool

    # unbalanced -> unbalanced
    pt(A, Grouped(A_sample, rev=isreversed, scaled_by_same_as=res, balanced=false))
    pt(res, BlockedAlong() & Unbalanced(A), match=A, on=["key", "divisions", "id"])

    # balanced -> balanced
    pt(A, Grouped(A_sample, rev=isreversed, balanced=true))
    pt(res, BlockedAlong() & Balanced(), match=A, on=["key", "divisions", "id"])

    # replicated
    pt(A, res, dims, kwargs, Replicated())
end

function _sortslices(A::Future, sortingdim::Int64, res::Future, dims::Future, kwargs::Future)::Future
    partitioned_with(pts_for_sortslices, [A, res, dims, kwargs], scaled=[A, res], keys=[sortingdim])

    @partitioned A dims kwargs res begin
        res = Base.sortslices(A, dims=dims, kwargs...)
    end

    res
end

function Base.sortslices(A::Array{T,N}, dims; kwargs...)::Array{T,N} where {T,N}
    @nospecialize

    get(kwargs, :by, identity)::Function == identity || throw(ArgumentError("Sorting by a function is not supported"))
    !haskey(kwargs, :order) || throw(ArgumentError("Sorting by an order is not supported"))

    # Determine what to sort by and whether to sort in reverse
    sortingdim::Int64 = dims isa Colon ? 1 : convert(Int64, first(dims))::Int64

    res_size = deepcopy(A.size)
    res = Future(datatype="Array")
    dims = Future(dims)
    kwargs = Future(kwargs)

    res = _sortslices(A.data, sortingdim, res, dims, kwargs)

    Array{T,N}(res, res_size)
end

Base.sort(A::Array{T,N}; kwargs...) where {T,N} = sortslices(A, dims=Colon(); kwargs...)

# Array aggregation

# TODO: Maybe add mean function or add this in BanyanStatsBase.jl
# function mean(V::Vector)
#     # TODO: Accept vector partitioned in different dimensions and return
#     # replicated with reducing or distributed with same id potentially grouped
#     # by an axis or balanced
#     sum(V) / length(V)
# end

# # Array element-wise operations

# # TODO: Implement a bunch of operations where we allow Replicated and Blocked
# # across a dimension where communication is not needed (e.g., accumulation
# # across dims=2 allows us to have Blocked along=1)

# Array unary operations

@nospecialize

for op in [:-]
    @eval begin
        Base.$op(X::Array{T,N}) where {T,N} = map($op, X)
    end
end

for (op, agg) in [(:(sum), :(+)), (:(minimum), :(min)), (:(maximum), :(max))]
    # TODO: Maybe try ensuring that the Base.:+ here is not including the method from above
    @eval begin
        Base.$op(X::Array{T,N}; dims=:) where {T,N} = reduce($agg, X; dims=dims)
    end
end

# Array binary operations

# for op in [:+, :-, :>, :<, :(>=), :(<=), :(==), :!=]
# NOTE: Only addition and subtraction are supported
for op in [:+, :-]
    @eval begin
        Base.$op(A::Array{T,N}, B::Array{T,N}) where {T,N} = map($op, A, B)
    end
end

@specialize

# TODO: Add broadcasting support
# TODO: Add reshaping
