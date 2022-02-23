function Base.copy(fut::AbstractFuture)
    res = Future(fut)

    # partitioned_using() do
    #     keep_all_sample_keys(res, fut)
    #     keep_sample_rate(res, fut)
    # end

    # TODO: Ensure that the CopyTo function can always be used to split anything
    # for which there is no specified PT. But make sure that other splitting
    # functions such as those that split arrays are expecting a name to be
    # provided. If we do that, we can un-comment the below and allow for
    # copying anything  
    # pt(fut, PartitionType())
    # pt(res, PartitionType(), match=fut)
    partitioned_with(scaled=[fut, res]) do
        pt(fut, Replicating())
        pt(res, Replicating(), match=fut)
    end

    @partitioned fut res begin
        res = Base.copy(fut)
    end

    res
end

function Base.deepcopy(fut::AbstractFuture)
    res = Future(fut)

    # partitioned_using() do
    #     keep_all_sample_keys(res, fut)
    #     keep_sample_rate(res, fut)
    # end

    # TODO: Ensure that the CopyTo function can always be used to split anything
    # for which there is no specified PT. But make sure that other splitting
    # functions such as those that split arrays are expecting a name to be
    # provided. If we do that, we can un-comment the below and allow for
    # copying anything  
    # pt(fut, PartitionType())
    # pt(res, PartitionType(), match=fut)
    partitioned_with(scaled=[fut, res]) do
        pt(fut, Replicating())
        pt(res, Replicating(), match=fut)
    end

    @partitioned fut res begin
        res = Base.deepcopy(fut)
    end

    res
end

# Array type

struct Array{T,N} <: AbstractFuture where {T,N}
    data::Future
    size::Future
    # TODO: Add offset for indexing
    # offset::Future

    Array{T,N}() where {T,N} = new(Future(datatype="Array"), Future())
    Array{T,N}(A::Array{T,N}) where {T,N} = new(Future(datatype="Array"), Future(A.size))
    Array{T,N}(data::Future, size::Future) where {T,N} = new(data, size)
end

const Vector{T} = Array{T,1}
const Matrix{T} = Array{T,2}

Base.convert(::Type{Array{T}}, A::AbstractArray{T,N}) where {T,N} = Array{T,N}(Future(A, datatype="Array"), Future(size(A)))
Base.convert(::Type{Array}, arr::AbstractArray{T}) where {T} = convert(Array{T}, arr)
Base.convert(::Type{Vector{T}}, arr::AbstractVector) where {T} = convert(Array{T}, arr)

Banyan.convert(::Type{Future}, A::Array{T,N}) where {T,N} = A.data

# Array sample

Banyan.sample_axes(A::U) where U <: Base.AbstractArray{T,N} where {T,N} = [1:ndims(A)...]
Banyan.sample_keys(A::U) where U <: Base.AbstractArray{T,N} where {T,N} = sample_axes(A)

# `sample_divisions`, `sample_percentile`, and `sample_max_ngroups` should
# work with the `orderinghash` of values in the data they are used on

function Banyan.sample_divisions(A::U, key) where U <: Base.AbstractArray{T,N} where {T,N}
    if isempty(A)
        return []
    end

    max_ngroups = sample_max_ngroups(A, key)
    ngroups = min(max_ngroups, 512)
    data = sort([orderinghash(e) for e in eachslice(A, dims=key)])
    datalength = length(data)
    grouplength = div(datalength, ngroups)
    # We use `unique` here because if the divisions have duplicates, this could
    # result in different partitions getting the same divisions.
    # TODO: Ensure that `unique` doesn't change the order
    unique([
        # Each group has elements that are >= start and < end
        (
            data[(i-1)*grouplength + 1],
            data[i == ngroups ? datalength : i*grouplength + 1]
        )
        for i in 1:ngroups
    ])
end

function Banyan.sample_percentile(A::U, key, minvalue, maxvalue) where U <: Base.AbstractArray{T,N} where {T,N}
    if isempty(A) || isnothing(minvalue) || isnothing(maxvalue)
        return 0
    end

    count((begin oh = orderinghash(e); oh >= minvalue && oh <= maxvalue end for e in eachslice(A, dims=key))) / size(A, key)

    # TODO: Determine whether we need to assume a more coarse-grained percentile using the divisions
    # from `sample_divisions` and computing the percent of divisions that overlap with the range

    # # minvalue, maxvalue = orderinghash(minvalue), orderinghash(maxvalue)
    # divisions = sample_divisions(A, key)
    # percentile = 0
    # divpercentile = 1/length(divisions)
    # inminmax = false

    # # Iterate through divisions to compute percentile
    # for (i, (divminvalue, divmaxvalue)) in enumerate(divisions)
    #     # Check if we are between the minvalue and maxvalue
    #     if (i == 1 || minvalue >= divminvalue) && (i == length(divisions) || minvalue < divmaxvalue)
    #         inminmax = true
    #     end

    #     # Add to percentile
    #     if inminmax
    #         percentile += divpercentile
    #     end

    #     # Check if we are no longer between the minvalue and maxvalue
    #     if (i == 1 || maxvalue >= divminvalue) && (i == length(divisions) || maxvalue < divmaxvalue)
    #         inminmax = false
    #     end
    # end

    # percentile
end

# NOTE: The key used for arrays must be a single dimension - if you are doing
# something like sorting on multiple dimensions or grouping on multiple
# dimensions you can just use the first dimension as the key.

function Banyan.sample_max_ngroups(A::U, key) where U <: Base.AbstractArray{T,N} where {T,N}
    if isempty(A)
        return 0
    end

    data = sort([orderinghash(e) for e in eachslice(A, dims=key)])
    currgroupsize = 1
    maxgroupsize = 0
    prev = nothing
    prev_is_nothing = true # in case `prev` _can_ be nothing
    for curr in data
        if !prev_is_nothing && curr == prev
            currgroupsize += 1
        else
            maxgroupsize = max(maxgroupsize, currgroupsize)
            currgroupsize = 1
        end
        # TODO: Maybe use deepcopy here if eltype might be nested
        prev = copy(curr)
        prev_is_nothing = false
    end
    maxgroupsize = max(maxgroupsize, currgroupsize)
    div(size(A, key), maxgroupsize)
end
# TODO: Handle issue where mapslices requires dims to be a single dimension;
# probably need to vary mapslices on the dim itself and then use eachslices,
# get the orderinghash and then take minimum across that
# TODO: Change to use eachslice everywhere and ensure we use key not d
Banyan.sample_min(A::U, key) where U <: Base.AbstractArray{T,N} where {T,N} = isempty(A) ? nothing : minimum((orderinghash(e) for e in eachslice(A, dims=key)))
Banyan.sample_max(A::U, key) where U <: Base.AbstractArray{T,N} where {T,N} = isempty(A) ? nothing : maximum((orderinghash(e) for e in eachslice(A, dims=key)))

# Array creation

function Banyan.compute_inplace(A::Array{T,N}) where {T,N}
    partitioned_computation(A, destination=Disk()) do
        pt(A, Blocked(A) | Replicated())
    end
end

function fill(v, dims::NTuple{N,Integer}) where {N}
    fillingdims = Future(source=Size(dims))
    A = Array{typeof(v),N}(Future(datatype="Array"), Future(dims))
    v = Future(v)
    dims = Future(dims)

    # For futures that contains dims or nrows, we initialize them to store the
    # dimensions or # of rows of the whole value. Then if we need to mutate,
    # we reassign to the dimensions or # of rows of the part and assign a
    # reducing PT.

    # NOTE: If a value is being created for the first time in a code region, it
    # is being mutated. The only exception is a value that is already created
    # on the client and is merely being replicated in the code region.
    # partition(A, Replicated())
    # partition(dims, Replicated())
    
    # for axis in 1:min(4, ndims(A))
    #     # Partition with distribution of either balanced, grouped, or unknown
    #     partition(A, Blocked(key=a))
    # end
    # We use `partitioned_with` here to ensure that a sample of A is produced
    # first so that we can use the Blocked PT constructor which depends on A
    # having its sample taken
    partitioned_with(scaled=A) do
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

    # TODO: Remove the println @macroexpand
    # println(@macroexpand begin @partitioned A v fillingdims begin
    #     println(A)
    #     println(v)
    #     A = fill(v, fillingdims)
    # end end)
    @partitioned A v fillingdims begin
        A = Base.fill(v, fillingdims)
    end

    A
end

fill(v, dims::Integer...) = fill(v, Tuple(dims))

function collect(r::AbstractRange)
    # Create output futures
    r = Future(r)
    A = Future(datatype="Array")

    # Define how the data can be partitioned (mentally taking into account
    # data imbalance and grouping)
    partitioned_with(scaled=A) do
        pt(A, Blocked(A, balanced=true))
        pt(r, Divided())
        pt(A, r, Replicated())
    end

    # Offload the partitioned computation
    @partitioned r A begin A = Base.collect(r) end

    # Return a Banyan vector as the result
    Vector{eltype(compute(r))}(A, Future((length(compute(r)),)))
end

zeros(::Type{T}, args...; kwargs...) where {T} = fill(zero(T), args...; kwargs...)
zeros(args...; kwargs...) where {T} = zeros(Float64, args...; kwargs...)
ones(::Type{T}, args...; kwargs...) where {T} = fill(one(T), args...; kwargs...)
ones(args...; kwargs...) where {T} = ones(Float64, args...; kwargs...)
trues(args...; kwargs...) where {T} = fill(true, args...; kwargs...)
falses(args...; kwargs...) where {T} = fill(false, args...; kwargs...)

# Array properties

Base.ndims(A::Array{T,N}) where {T,N} = ndims(sample(A))
Base.size(A::Array{T,N}) where {T,N} = compute(A.size)
Base.length(V::Array{T,N}) where {T,N} = prod(compute(V.size))
Base.eltype(A::Array{T,N}) where {T,N} = eltype(sample(A))

function pts_for_copying(A, res)
    # balanced
    pt(A, Blocked(A, balanced=true))
    pt(res, Balanced(), match=A)

    # unbalanced
    pt(A, Blocked(A, balanced=false, scaled_by_same_as=res))
    pt(res, Unbalanced(scaled_by_same_as=A), match=A)
    
    # replicated
    pt(A, res, Replicated())
end

function Base.copy(A::Array{T,N})::Array{T,N} where {T,N}
    res = Future(datatype="Array")

    partitioned_with(scaled=[A, res], keep_same_keys=true) do
        pts_for_copying(A, res)
    end

    @partitioned A res begin
        res = Base.copy(A)
    end

    Array{T,N}(res, deepcopy(A.size))
end

function Base.deepcopy(A::Array{T,N})::Array{T,N} where {T,N}
    res = Future(datatype="Array")

    partitioned_with(scaled=[A, res], keep_same_keys=true) do
        pts_for_copying(A, res)
    end

    @partitioned A res begin
        res = Base.deepcopy(A)
    end

    Array{T,N}(res, deepcopy(A.size))
end

# Array operations

function Base.map(f, c::Array{<:Any,N}...; force_parallelism=false) where {T,N}
    # We shouldn't need to keep sample keys since we are only allowing data
    # to be blocked for now. The sample rate is kept because it might be
    # smaller if this is a column of the result of a join operation.
    # keep_all_sample_keys(res, fut)

    # TODO: Determine whether array operations need to use mutated_from or mutated_to
    # TODO: Instead just make the Array constructor have a code region using
    # the data in a replicated way.

    if force_parallelism
        # If we are forcing parallelism, we have an empty code region to
        # allow for copying from sources like client side and then casting
        # from replicated partitioning to distributed partitioning

        for c_arg in c
            mutated(c_arg)
        end

        partitioned_with(scaled=[c...]) do
            # balanced
            pt(first(c), Blocked(first(c), balanced=true))
            pt(c[2:end]..., Blocked() & Balanced(), match=first(c), on=["key"])
    
            # unbalanced
            pt(first(c), Blocked(first(c)))
            pt(c[2:end]..., ScaledBySame(as=first(c)), match=first(c))

            # replicated
            pt(c..., Replicated())
        end

        @partitioned c begin end
    end

    f = Future(f)
    res = Future(datatype="Array")    

    partitioned_with(scaled=[res, c...]) do
        # balanced
        pt(first(c), Blocked(first(c), balanced=true))
        pt(c[2:end]..., Blocked() & Balanced(), match=first(c), on=["key"])
        pt(res, Blocked() & Balanced(), match=first(c))

        # unbalanced
        pt(first(c), Blocked(first(c)))
        pt(c[2:end]..., ScaledBySame(as=first(c)), match=first(c))
        pt(res, ScaledBySame(as=first(c)), match=first(c))

        # replicated
        if force_parallelism
            pt(f, Replicated())
        else
            pt(c..., res, f, Replicated())
        end
    end

    # println(@macroexpand begin @partitioned f c res begin
    #     res = Base.map(f, c...)
    # end end)
    @partitioned f c res begin
        res = Base.map(f, c...)
        # @show res
        # @show typeof(res)
        # @show eltype(res)
    end

    # @show sample(res)
    # @show typeof(sample(res))
    # @show eltype(sample(res))

    Array{eltype(sample(res)),N}(res, deepcopy(first(c).size))
end

function Base.mapslices(f, A::Array{T,N}; dims) where {T,N}
    if isempty(dims) return map(f, A) end

    f = Future(f)
    res_size = Future()
    # TODO: Ensure that this usage of Any is correct here and elsewhere
    res = Array{Any,Any}(Future(datatype="Array"), res_size)
    dims = Future(dims)

    partitioned_with(scaled=[A, res]) do
        # Blocked PTs along dimensions _not_ being mapped along
        bpt = [bpt for bpt in Blocked(A) if !(compute(dims) isa Colon) && !(bpt.key in [compute(dims)...])]

        if !isempty(bpt)
            # balanced
            pt(A, bpt & Balanced())
            pt(res, Blocked() & Balanced(), match=A, on="key")

            # unbalanced
            pt(A, bpt & Unbalanced(scaled_by_same_as=res))
            pt(res, Unbalanced(scaled_by_same_as=A), match=A)
        end

        # replicated
        # TODO: Determine why this MatchOn constraint is not propagating
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> if !isnothing(a) && !isnothing(b) Banyan.indexapply(+, a, b, index=axis) else isnothing(a) ? b : a end end), match=A, on="key")
        pt(A, res, res_size, f, dims, Replicated())
    end

    @partitioned f A dims res res_size begin
        # We return nothing because `mapslices` doesn't work properly for
        # empty data
        res = isempty(A) ? nothing : Base.mapslices(f, A, dims=dims)
        res_size = isempty(A) ? nothing : Base.size(res)
    end

    res
end

# function getindex_size(A_s, indices...)
#     if length(indices) == 1
#         # Linear-indexing case
#         if indices[1] isa Colon
#             prod(A_s)
#         elseif indices[1] isa Vector
#             length(indices[1])
#         else
#             # Accessing a single element
#             1
#         end
#     else
#         # Multi-dimensional indexing case
#         if all((i isa Integer for i in indices))
#             # Accessing a single element
#             1
#         else
#             tuple(
#                 [
#                     if indices[i] isa Colon
#                         s
#                     else 
#                         length(indices[i])
#                     end
#                     for (i, s) in enumerate(A_s)
#                     if indices[i] isa Colon || indices[i] isa Vector
#                 ]
#             )
#         end
#     end
# end

function Base.getindex(A::Array{T,N}, indices...) where {T,N}
    # If we are doing linear indexing, then the data can only be split on the
    # last dimension because of the column-major ordering
    all((i isa Colon || i isa Integer || i isa Vector for i in indices)) || error("Expected indices to be either integers, vectors of integers, or colons")
    allowed_splitting_dims = if length(indices) == 1 && indices[1] isa Colon
        [ndims(A)]
    elseif length(indices) == ndims(A)
        [i for i in 1:ndims(A) if indices[i] isa Colon]
    else
        []
    end

    indices = Future(indices)
    # A_size = A.size
    # res_size = Future(A_size, mutation=A_s->getindex_size(A_s, indices...))
    res_size = Future()
    res = Future(datatype="Array")

    partitioned_with(scaled=[A, res]) do 
        # Blocked PTs along dimensions _not_ being mapped along
        bpt = [bpt for bpt in Blocked(A) if bpt.key in allowed_splitting_dims]

        if !isempty(bpt)
            # balanced
            pt(A, bpt & Balanced())
            pt(res, Blocked() & Balanced(), match=A, on="key")

            # unbalanced
            pt(A, bpt & Unbalanced(scaled_by_same_as=res))
            pt(res, Unbalanced(scaled_by_same_as=A), match=A)

            # # Keep the same kind of size
            # pt(A_size, Replicating())
            # pt(res_size, PartitionType(), match=A_size)
            # TODO: See if `quote` is no longer needed
            pt(res_size, ReducingWithKey(quote axis -> (a, b) -> if !isnothing(a) && !isnothing(b) Banyan.indexapply(+, a, b, index=axis) else isnothing(a) ? b : a end end), match=A, on="key")
        end

        pt(A, res, res_size, indices, Replicated())
    end

    # TODO: Add back in A_size and try to use it with mutation= in the `Future`
    # constructor to avoid having to do a reduction to compute size
    @partitioned A indices res res_size begin
        res = Base.getindex(A, indices...)
        # res_size = BanyanArrays.getindex_size(A_size, indices...)
        if res isa AbstractArray
            res_size = size(res)
        end
    end

    if sample(res) isa AbstractArray
        Array{eltype(sample(res)),ndims(sample(res))}(res, res_size)
    else
        res
    end
end

# TODO: Implement reduce and sortslices

function Base.reduce(op, A::Array{T,N}; dims=:, kwargs...) where {T,N}
    if :init in keys(kwargs) throw(ArgumentError("Reducing with an initial value is not currently supported")) end

    op = Future(op)
    res_size = Future()
    res = dims isa Colon ? Future() : Array{Any,Any}(Future(datatype="Array"), res_size)
    dims = Future(dims)
    kwargs = Future(kwargs)

    partitioned_with(scaled=[A, res]) do
        # TODO: Duplicate annotations to handle the balanced and unbalanced cases
        # seperately
        # TODO: Have a better API where duplicating to handle balanced and unbalanced
        # isn't needed
        # TODO: Cascaade MatchOn constraints
        # TODO: Ensure that MatchOn value is being discovered

        for bpt in Blocked(A)
            pt(A, bpt)
            if compute(dims) isa Colon || bpt.key in [compute(dims)...]
                # NOTE: Be careful about trying to serialize things that would
                # require serializing the whole Banyan module. For example, if
                # this where Reducing(op) or if we tried Future(op) where op
                # could refer to a + function overloaded by BanyanArrays.
                pt(res, Reducing(compute(op)))
            else
                pt(res, bpt.balanced ? Balanced() : Unbalanced(scaled_by_same_as=A), match=A)
            end
        end
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> Banyan.indexapply(+, a, b, index=axis) end), match=A, on="key")
        # TODO: Allow replication
        if !is_debug_on()
            pt(A, res, res_size, dims, kwargs, op, Replicated())
        else
            pt(dims, kwargs, op, Replicated())
        end
    end

    @partitioned op A dims kwargs res res_size begin
        if is_debug_on()
            # @show size(A)
            # @show dims # TODO: Figure out why dims is sometimes a function
        end
        res = Base.reduce(op, A; dims=dims, kwargs...)
        if res isa AbstractArray
            res_size = Base.size(res)
        end
    end

    res
end

function Base.sortslices(A::Array{T,N}, dims; kwargs...) where {T,N}
    get(kwargs, :by, identity) == identity || throw(ArgumentError("Sorting by a function is not supported"))
    !haskey(kwargs, :order) || throw(ArgumentError("Sorting by an order is not supported"))

    # Determine what to sort by and whether to sort in reverse
    sortingdim = dims isa Colon ? 1 : first(dims)
    isreversed = get(kwargs, :rev, false)

    res = Future(datatype="Array")
    dims = Future(dims)
    kwargs = Future(kwargs)

    partitioned_with(scaled=[A, res], keys=sortingdim) do
        # unbalanced -> unbalanced
        pt(A, Grouped(A, by=sortingdim, rev=isreversed, scaled_by_same_as=res, balanced=false))
        pt(res, Blocked() & Unbalanced(scaled_by_same_as=A), match=A, on=["key", "divisions", "id"])

        # balanced -> balanced
        pt(A, Grouped(A, by=sortingdim, rev=isreversed, balanced=true))
        pt(res, Blocked() & Balanced(), match=A, on=["key", "divisions", "id"])

        # replicated
        pt(A, res, dims, kwargs, Replicated())
    end

    @partitioned A dims kwargs res begin
        res = Base.sortslices(A, dims=dims, kwargs...)
    end

    Array{T,N}(res, deepcopy(A.size))
end

Base.sort(A::Array{T,N}; kwargs...) where {T,N} = sortslices(A, dims=:; kwargs...)

# Array aggregation

# TODO: Determine split between what happens in annotations vs PT constructors
# - Annotations
#   - Collecting axes/keys that are relevant to the computation by looking at arguments
#   - Iterating through keys that require special partitioning
#   - Registering a delayed PA and passing in job with info
# - PT Library
#   - Maintaining job_properties with keys and axes that are used

# for (op, agg) in [(:(sum), :(Base.:+)), (:(minimum), :(min)), (:(maximum), :(max))]
#     @eval begin
#         function $op(A::Array{T, N})
#             dims = dims isa Tuple ? dims : tuple(dims)
#             dimensions = Future(dims)
#             operator = Future($op)
#             res = Future()
#         end

#         function $op(A::Array{T, N}; dims=:) where {T, N}
#             dims = dims isa Tuple ? dims : tuple(dims)
#             dimensions = Future(dims)
#             operator = Future($op)
#             res_size = Future() # TODO: Compute this here using Future, mutation=
#             res = Array{T, N}(Future(), res_size)

#             partition(A, Replicated())
#             partition(res, Replicated())

#             # Partition by distributing across 1 of up to 4 axes
#             for axis in 1:min(4, ndims(A))
#                 # Partition with distribution of either balanced, grouped, or unknown
#                 partition(A, Blocked(key=axis))

#                 # Partition the result based on whether the reduction is across the dimension of partitioning
#                 if axis in dims
#                     partition(res, Reducing(reducer=$agg))
#                 else
#                     partition(res, Partitioned(), matches_with=A)
#                 end
#             end

#             # Partition all as replicated
#             partition(operator, Replicated())
#             partition(res_size, Replicated())

#             @partitioned A dimensions operator aggregaton res res_size begin
#                 res = $op(V, dims=dimensions)
#                 res_size = size(res)
#             end
#         end
#     end
# end

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

# TODO: Add broadcasting support

# # Binary operators
# for op in (:+, :-, :>, :<, :>=, :<=, :≥, :≤, :(==), :!=)
#     @eval begin
#         function Base.$op(A::Array{T, N}, B::Array{T, N}) where {T, N}
#             A_size = A.size
#             res_size = Future(A_size)
#             res = Array{T, N}(Future(), res_size)
#             op = Future(op)
#             # TODO:
#             # - make samples get computed instantly
#             # TODO: Put info in partitioned:
#             # - Future
#             # - PT (replicated, balanced, grouped, pseudo)
#             # - PCs (matching on, at most)
#             # - location (None) with sample, sample properties, total memory
#             # - mutating (constant)
#             # TODO: for columns: how to partition on grouped
#             # - maintain set of possible keys to group by
#             # - compute atmost whenever needed
#             # TODO: Implement new Distributed to support group-by views - how to partition on unknown?
#             # - distribution = balanced by axis|grouped by axis or key with divisions|unknown
#             # - identified by id = true|false
#             # - axis
#             # - key
#             # - divisions - ensure that a split function can't be used if it is required but not provided
#             # - ID (must be null for splitting or casting to)
#             # TODO: Auto-generate default random on casts and splits
#             # "!" for parameters that are to be randomly assigned, can be used both in PAs and in PT library
            
#             # TODO: Accept replicated or balanced or distributed with same ID for inputs and outputs while grouped by any valid key

#             partition(A, Replicated())
#             for axis in 1:min(4, ndims(A))
#                 partition(A, Blocked(key=axis))
#             end
#             partition(B, Partitioned(), matches_with=A)
#             partition(res, Partitioned(), matches_with=A, mutating=true)
#             partition(res_size, ReplicatedOrReducing(), match=A_size)
#             partition(op, Replicated())

#             # colnames = propertynames(sample(df))
#             # partition() do job
#             #     job.properties[:keys]
#             #     Grouped(propertynames(sample(df)))
#             #     Grouped(df, groupingkey, reverse=false)
#             # end
#             # partition(A, Balanced())
#             # partition(A, Distributed(like=B))
#             # partition(A_size, Replicated())
#             # partition(B, Distributed(), matches_with=A)
#             # partition(res, Distributed(), matches_with=A)
#             # partition(res_nrows, Replicated())
#             # partition(op, Replicated())

#             @partitioned res res_size A A_size B op begin
#                 res = op(A, B)
#                 res_size = A_size
#             end

#             res
#         end
#     end
# end

# # TODO: Implement unary operators

# # TODO: Implement element-wise operations

# # Array reshaping

# # TODO: Support sorting once we have a non-verbose way of specifying grouping for both dataframes and arrays

# # function sort(A::Array; kwargs...)
# #     # TODO: Accept replicated or grouped on key used for sotring and return same but with same ID but
# #     # newly generated ID if the input has id set to null because of casting or re-splitting
# #     # TODO: Accept A as either blocked on some other dimension or grouped on the dimension
# #     # used for sorting. Then have the result be blocked. And then use Blocked everywhere else and
# #     # ensure that getindex will only return Vector that is Blocked and not Grouped
# #     res = Future()
# #     partition(A, Grouped())
# # end

# function sortslices(A::Array)
#     # TODO: use the first of the dims to sort on to split the array
#     # TODO: use the first of the slice when splitting the array on the dim to get quantiles
# end