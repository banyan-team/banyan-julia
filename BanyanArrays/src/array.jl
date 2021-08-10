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
    pt(fut, Replicating())
    pt(res, Replicating(), match=fut)

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
    pt(fut, Replicating())
    pt(res, Replicating(), match=fut)

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

    Array{T,N}() where {T,N} = new(Future(), Future())
    Array{T,N}(A::Array{T,N}) where {T,N} = new(Future(), Future(A.size))
    Array{T,N}(data::Future, size::Future) where {T,N} = new(data, size)
end

const Vector{T} = Array{T,1}
const Matrix{T} = Array{T,2}

Banyan.convert(::Type{Future}, A::Array{T,N}) where {T,N} = A.data

# Array sample

Banyan.sample_axes(A::Base.Array{T,N}) where {T,N} = [1:ndims(A)...]
Banyan.sample_keys(A::Base.Array{T,N}) where {T,N} = sample_axes(A)

# `sample_divisions`, `sample_percentile`, and `sample_max_ngroups` should
# work with the `orderinghash` of values in the data they are used on

# NOTE: This is duplicated between pt_lib.jl and the client library
orderinghash(x::Any) = x # This lets us handle numbers and dates
orderinghash(s::String) = Integer.(codepoint.(collect(first(s, 32) * repeat(" ", 32-length(s)))))
orderinghash(A::Array) = orderinghash(first(A))

function Banyan.sample_divisions(A::Base.Array{T,N}, key) where {T,N}
    max_ngroups = sample_max_ngroups(df, key)
    ngroups = min(max_ngroups, Banyan.get_job().nworkers * 8, 128)
    data = sort(mapslices(first, transpose(A), dims=key))
    datalength = length(data)
    grouplength = div(datalength, ngroups)
    [
        # Each group has elements that are >= start and < end
        (
            orderinghash(data[(i-1)*grouplength + 1]),
            orderinghash(data[i == ngroups ? datalength : i*grouplength + 1])
        )
        for i in 1:ngroups
    ]
end

function Banyan.sample_percentile(A::Base.Array{T,N}, key, minvalue, maxvalue) where {T,N}
    minvalue, maxvalue = orderinghash(minvalue), orderinghash(maxvalue)
    divisions = sample_divisions(A, key)
    percentile = 0
    divpercentile = 1/length(divisions)
    inminmax = false

    # Iterate through divisions to compute percentile
    for (i, (divminvalue, divmaxvalue)) in enumerate(divisions)
        # Check if we are between the minvalue and maxvalue
        if (i == 1 || minvalue >= divminvalue) && (i == length(divisions) || minvalue < divmaxvalue)
            inminmax = true
        end

        # Add to percentile
        if inminmax
            percentile += divpercentile
        end

        # Check if we are no longer between the minvalue and maxvalue
        if (i == 1 || maxvalue >= divminvalue) && (i == length(divisions) || maxvalue < divmaxvalue)
            inminmax = false
        end
    end

    percentile
end

Banyan.sample_max_ngroups(A::Base.Array{T,N}, key) where {T,N} =
    begin
        data = sort(mapslices(orderinghash, transpose(A), dims=key))
        currgroupsize = 1
        maxgroupsize = 0
        prev = nothing
        for curr in data
            if curr == prev
                currgroupsize += 1
            else
                maxgroupsize = max(maxgroupsize, currgroupsize)
                currgroupsize = 1
            end
            # TODO: Maybe use deepcopy here if eltype might be nested
            prev = copy(curr)
        end
        maxgroupsize = max(maxgroupsize, currgroupsize)
        div(size(df, key), maxgroupsize)
    end
Banyan.sample_min(A::Base.Array{T,N}, key) where {T,N} = minimum(mapslices(first, transpose(A), dims=key))
Banyan.sample_max(A::Base.Array{T,N}, key) where {T,N} = maximum(mapslices(first, transpose(A), dims=key))

# Array creation

function read_hdf5(path)
    A_loc = Remote(path)
    if is_debug_on()
        @show A_loc.src_parameters
        @show A_loc.size
    end
    A = Future(A_loc)
    Array{A_loc.eltype,A_loc.ndims}(A, Future(A_loc.size))
end

function write_hdf5(A, path)
    # A_loc = Remote(pathname, mount)
    destined(A, Remote(path, delete_from_cache=true))
    mutated(A)
    # This doesn't rely on any sample properties so we don't need to wrap this
    # in a `partitioned_with` to delay the PT construction to after sample
    # properties are computed.
    pt(A, Blocked(A) | Replicated())
    # partition(A, Replicated()) # TODO: Use semicolon for keyword args
    # Distributed will allow for balanced=true|false and any divisions or key
    # but the PT library should be set up such that you can't split if
    # divisions or key are not provided or balanced=false
    # partition(A, Blocked())
    # for axis in 1:min(4, ndims(A))
    #     # Partition with distribution of either balanced, grouped, or unknown
    #     partition(A, Blocked(key=a), mutated=true)
    # end
    @partitioned A begin end
    compute(A)
end

function fill(v, dims::NTuple{N,Integer}) where {N}
    fillingdims = Future(Size(dims))
    A = Array{typeof(v),N}(Future(), Future(dims))
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
    partitioned_with() do
        # blocked
        # TODO: Ensure that we are properly creating new PAs
        pt(A, Blocked(A))
        pt(fillingdims, Divided(), match=A, on="key")

        # replicated
        pt(A, fillingdims, v, Replicated())
    end

    # TODO: Remove the println @macroexpand
    # println(@macroexpand begin @partitioned A v fillingdims begin
    #     println(A)
    #     println(v)
    #     A = fill(v, fillingdims)
    # end end)
    if is_debug_on()
        @partitioned A v fillingdims begin
            A = Base.fill(v, fillingdims)
            println(size(A))
        end
    else
        @partitioned A v fillingdims begin
            A = Base.fill(v, fillingdims)
        end
    end

    A
end

fill(v, dims::Integer...) = fill(v, Tuple(dims))

zeros(::Type{T}, args...; kwargs...) where {T} = fill(zero(T), args...; kwargs...)
zeros(args...; kwargs...) where {T} = zeros(Float64, args...; kwargs...)
ones(::Type{T}, args...; kwargs...) where {T} = fill(one(T), args...; kwargs...)
ones(args...; kwargs...) where {T} = ones(Float64, args...; kwargs...)
trues(args...; kwargs...) where {T} = fill(true, args...; kwargs...)
falses(args...; kwargs...) where {T} = fill(false, args...; kwargs...)

# Array properties

Base.ndims(A::Array{T,N}) where {T,N} = ndims(sample(A))
Base.size(A::Array{T,N}) where {T,N} = collect(A.size)
Base.length(V::Array{T,N}) where {T,N} = prod(collect(V.size))
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
    res = Future()

    partitioned_using() do
        keep_all_sample_keys(res, A)
        keep_sample_rate(res, A)
    end

    partitioned_with() do
        pts_for_copying(A, res)
    end

    @partitioned A res begin
        res = Base.copy(A)
    end

    Array{T,N}(res, deepcopy(A.size))
end

function Base.deepcopy(A::Array{T,N})::Array{T,N} where {T,N}
    res = Future()

    partitioned_using() do
        keep_all_sample_keys(res, A)
        keep_sample_rate(res, A)
    end

    partitioned_with() do
        pts_for_copying(A, res)
    end

    @partitioned A res begin
        res = Base.deepcopy(A)
    end

    Array{T,N}(res, deepcopy(A.size))
end

# Array operations

function Base.map(f, c::Array{T,N}...) where {T,N}
    f = Future(f)
    res = Future()

    partitioned_using() do
        # We shouldn't need to keep sample keys since we are only allowing data
        # to be blocked for now. The sample rate is kept because it might be
        # smaller if this is a column of the result of a join operation.
        # keep_all_sample_keys(res, fut)
        keep_sample_rate(res, first(c))
    end

    # TODO: Determine whether array operations need to use mutated_from or mutated_to

    partitioned_with() do
        # balanced
        pt(first(c), Blocked(first(c), balanced=true))
        pt(c[2:end]..., res, Blocked() & Balanced(), match=first(c), on="key")

        # unbalanced
        pt(first(c), Blocked(first(c), balanced=false, scaled_by_same_as=res))
        pt(c[2:end]..., res, Unbalanced(scaled_by_same_as=first(c)), match=first(c))

        # replicated
        if !is_debug_on()
            pt(c..., res, f, Replicated())
        else
            pt(f, Replicated())
        end
    end

    # println(@macroexpand begin @partitioned f c res begin
    #     res = Base.map(f, c...)
    # end end)
    @partitioned f c res begin
        res = Base.map(f, c...)
    end

    Array{T,N}(res, deepcopy(first(c).size))
end

# NOTE: This function is shared between the client library and the PT library
function indexapply(op, objs...; index::Integer=1)
    lists = [obj for obj in objs if (obj isa AbstractVector || obj isa Tuple)]
    length(lists) > 0 || throw(ArgumentError("Expected at least one tuple as input"))
    index = index isa Colon ? length(first(lists)) : index
    operands = [((obj isa AbstractVector || obj isa Tuple) ? obj[index] : obj) for obj in objs]
    indexres = op(operands...)
    res = first(lists)
    if first(lists) isa Tuple
        res = [res...]
        res[index] = indexres
        Tuple(res)
    else
        res = copy(res)
        res[index] = indexres
        res
    end
end

function Base.mapslices(f, A::Array{T,N}; dims) where {T,N}
    if isempty(dims) return map(f, A) end

    f = Future(f)
    res_size = Future()
    # TODO: Ensure that this usage of Any is correct here and elsewhere
    res = Array{Any,Any}(Future(), res_size)
    dims = Future(dims)

    partitioned_using() do
        keep_sample_rate(res, A)
    end

    partitioned_with() do
        # Blocked PTs along dimensions _not_ being mapped along
        bpt = [bpt for bpt in Blocked(A) if !(dims isa Colon) && !(bpt.key in [dims...])]

        # balanced
        pt(A, bpt & Balanced())
        pt(res, Blocked() & Balanced(), match=A, on="key")

        # unbalanced
        pt(A, bpt & Unbalanced(scaled_by_same_as=res))
        pt(res, Unbalanced(scaled_by_same_as=A), match=A)

        # replicated
        # TODO: Determine why this MatchOn constraint is not propagating
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> indexapply(+, a, b, index=axis) end), match=A, on="key")
        pt(A, res, res_size, f, dims, Replicated())
    end

    @partitioned f A dims res begin
        res = Base.mapslices(f, A, dims=dims)
        res_size = Base.size(res)
    end

    res
end

# TODO: Implement reduce and sortslices

function Base.reduce(op, A::Array{T,N}; dims=:, kwargs...) where {T,N}
    if :init in keys(kwargs) throw(ArgumentError("Reducing with an initial value is not currently supported")) end

    op = Future(op)
    res_size = Future()
    res = dims isa Colon ? Future() : Array{Any,Any}(Future(), res_size)
    if is_debug_on()
        @show dims # TODO: Ensure this isn't a function
    end
    dims = Future(dims)
    kwargs = Future(kwargs)

    partitioned_using() do
        keep_sample_rate(res, A)
    end

    partitioned_with() do
        # TODO: Duplicate annotations to handle the balanced and unbalanced cases
        # seperately
        # TODO: Have a better API where duplicating to handle balanced and unbalanced
        # isn't needed
        # TODO: Cascaade MatchOn constraints
        # TODO: Ensure that MatchOn value is being discovered

        for bpt in Blocked(A)
            pt(A, bpt)
            if collect(dims) isa Colon || bpt.key in [collect(dims)...]
                # NOTE: Be careful about trying to serialize things that would
                # require serializing the whole Banyan module. For example, if
                # this where Reducing(op) or if we tried Future(op) where op
                # could refer to a + function overloaded by BanyanArrays.
                pt(res, Reducing(collect(op)))
            else
                pt(res, bpt.balanced ? Balanced() : Unbalanced(scaled_by_same_as=A), match=A)
            end
        end
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> indexapply(+, a, b, index=axis) end), match=A, on="key")
        # TODO: Allow replication
        if !is_debug_on()
            pt(A, res, res_size, dims, kwargs, op, Replicated())
        else
            pt(dims, kwargs, op, Replicated())
        end
    end

    @partitioned op A dims kwargs res res_size begin
        if is_debug_on()
            @show size(A)
            @show dims # TODO: Figure out why dims is sometimes a function
        end
        res = Base.reduce(op, A; dims=dims, kwargs...)
        if res isa Array
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

    res = Future()
    dims = Future(dims)
    kwargs = Future(kwargs)

    partitioned_using() do
        keep_sample_rate(res, A)
    end

    partitioned_with() do
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
        res = sortslices(A, dims=dims, kwargs...)
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

# # function sort(A::Array; kwargs)
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