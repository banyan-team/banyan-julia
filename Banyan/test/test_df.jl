function Base.copy(fut::AbstractFuture)
    res = Future(fut)
    partition(fut, Partitioned())
    partition(res, Partitioned(), matches_with=fut)
    @partitioned fut res begin
        res = copy(fut)
    end
end

# Array type

struct Array{T,N} <: AbstractFuture
    data::Future
    size::Future
    # TODO: Add offset for indexing
    # offset::Future

    Array{T,N}() = new(Future(), Future())
    Array{T,N}(A::Array{T,N}) = new(Future(), Future(A.size))
end

convert(::Type{Future}, A::Array) = A.data

const Vector{T} = Array{T,1}
const Matrix{T} = Array{T,2}

# Array creation

function read_hdf5(pathname; mount=nothing)
    A_loc = Remote(pathname, mount)
    A_size = Future(A_loc.size)
    A_data = Future(A_loc)
    A_eltype = eltype(sample(A_data))
    A = Array{A_eltype,N}(A_data, A_size)
    A
end

function write_hdf5(A, pathname; mount=nothing)
    A_loc = Remote(pathname, mount)
    dst(A, A_loc)
    partition(A, Replicated()) # TODO: Use semicolon for keyword args
    # Distributed will allow for balanced=true|false and any divisions or key
    # but the PT library should be set up such that you can't split if
    # divisions or key are not provided or balanced=false
    partition(A, Distributed())
    # for axis in 1:min(4, ndims(A))
    #     # Partition with distribution of either balanced, grouped, or unknown
    #     partition(A, Blocked(key=a), mutated=true)
    # end
    @partitioned A begin end
    compute(A)
end

function fill(v, dims::NTuple{N,Integer}) where {N}
    v = convert(Future, v)
    dims = Future(dims)
    A = Array{typeof(v),N}(Future(), Future(dims))

    # NOTE: If a value is being created for the first time in a code region, it
    # is being mutated. The only exception is a value that is already created
    # on the client and is merely being replicated in the code region.
    partition(A, Replicated())
    partition(dims, Replicated())
    
    for axis in 1:min(4, ndims(A))
        # Partition with distribution of either balanced, grouped, or unknown
        partition(A, Blocked(key=a))
    end
    partition(dims, Divided(), matches_with=A, matches_on="key")
    partition(v, Replicated())

    mut(A)

    @partitioned A v dims begin
        A = fill(v, dims)
    end

    A
end

fill(v, dims::Integer...) = fill(v, tuple(dims))

# Array properties

ndims(A::Array) = ndims(sample(A))
size(A::Array) = compute(A.size)
length(A::Vector) = compute(A.size)[1]

# Array aggregation

# TODO: Determine split between what happens in annotations vs PT constructors
# - Annotations
#   - Collecting axes/keys that are relevant to the computation by looking at arguments
#   - Iterating through keys that require special partitioning
#   - Registering a delayed PA and passing in job with info
# - PT Library
#   - Maintaining job_properties with keys and axes that are used

for (op, agg) in [(:(sum), :(Base.:+)), (:(minimum), :(min)), (:(maximum), :(max))]
    @eval begin
        function $op(A::Array{T, N})
            dims = dims isa Tuple ? dims : tuple(dims)
            dimensions = Future(dims)
            operator = Future($op)
            res = Future()
        end

        function $op(A::Array{T, N}; dims=:) where {T, N}
            dims = dims isa Tuple ? dims : tuple(dims)
            dimensions = Future(dims)
            operator = Future($op)
            res_size = Future() # TODO: Compute this here using Future, mutation=
            res = Array{T, N}(Future(), res_size)

            partition(A, Replicated())
            partition(res, Replicated())

            # Partition by distributing across 1 of up to 4 axes
            for axis in 1:min(4, ndims(A))
                # Partition with distribution of either balanced, grouped, or unknown
                partition(A, Blocked(key=axis))

                # Partition the result based on whether the reduction is across the dimension of partitioning
                if axis in dims
                    partition(res, Reducing(reducer=$agg))
                else
                    partition(res, Partitioned(), matches_with=A)
                end
            end

            # Partition all as replicated
            partition(operator, Replicated())
            partition(res_size, Replicated())

            @partitioned A dimensions operator aggregaton res res_size begin
                res = $op(V, dims=dimensions)
                res_size = size(res)
            end
        end
    end
end

function mean(V::Vector)
    # TODO: Accept vector partitioned in different dimensions and return
    # replicated with reducing or distributed with same id potentially grouped
    # by an axis or balanced
    sum(V) / length(V)
end

# Array element-wise operations

# Binary operators
for op in (:+, :-, :>, :<, :>=, :<=, :≥, :≤, :(==), :!=)
    @eval begin
        function Base.$op(A::Array{T, N}, B::Array{T, N}) where {T, N}
            A_size = A.size
            res_size = Future(A_size)
            res = Array{T, N}(Future(), res_size)
            op = Future(op)
            # TODO:
            # - make samples get computed instantly
            # TODO: Put info in partitioned:
            # - Future
            # - PT (replicated, balanced, grouped, pseudo)
            # - PCs (matching on, at most)
            # - location (None) with sample, sample properties, total memory
            # - mutating (constant)
            # TODO: for columns: how to partition on grouped
            # - maintain set of possible keys to group by
            # - compute atmost whenever needed
            # TODO: Implement new Distributed to support group-by views - how to partition on unknown?
            # - distribution = balanced by axis|grouped by axis or key with divisions|unknown
            # - identified by id = true|false
            # - axis
            # - key
            # - divisions - ensure that a split function can't be used if it is required but not provided
            # - ID (must be null for splitting or casting to)
            # TODO: Auto-generate default random on casts and splits
            # "!" for parameters that are to be randomly assigned, can be used both in PAs and in PT library
            
            # TODO: Accept replicated or balanced or distributed with same ID for inputs and outputs while grouped by any valid key

            partition(A, Replicated())
            for axis in 1:min(4, ndims(A))
                partition(A, Blocked(key=axis))
            end
            partition(B, Partitioned(), matches_with=A)
            partition(res, Partitioned(), matches_with=A, mutating=true)
            partition(res_size, ReplicatedOrReducing(), match_with=A_size)
            partition(op, Replicated())

            # colnames = propertynames(sample(df))
            # partition() do job
            #     job.properties[:keys]
            #     Grouped(propertynames(sample(df)))
            #     Grouped(df, groupingkey, reverse=false)
            # end
            # partition(A, Balanced())
            # partition(A, Distributed(like=B))
            # partition(A_size, Replicated())
            # partition(B, Distributed(), matches_with=A)
            # partition(res, Distributed(), matches_with=A)
            # partition(res_nrows, Replicated())
            # partition(op, Replicated())

            @partitioned res res_size A A_size B op begin
                res = op(A, B)
                res_size = A_size
            end
            
            res
        end
    end
end

# TODO: Implement unary operators

# TODO: Implement element-wise operations

# Array reshaping

# TODO: Support sorting once we have a non-verbose way of specifying grouping for both dataframes and arrays

# function sort(A::Array; kwargs)
#     # TODO: Accept replicated or grouped on key used for sotring and return same but with same ID but
#     # newly generated ID if the input has id set to null because of casting or re-splitting
#     # TODO: Accept A as either blocked on some other dimension or grouped on the dimension
#     # used for sorting. Then have the result be blocked. And then use Blocked everywhere else and
#     # ensure that getindex will only return Vector that is Blocked and not Grouped
#     res = Future()
#     partition(A, Grouped())
# end

struct DataFrame <: AbstractFuture
    data::Future
    nrows::Future
    # TODO: Add offset for indexing
    # offset::Future
end

convert(::Type{Future}, df::DataFrame) = df.data

# DataFrame creation

# function read_csv(pathname)
#     location = CSVPath(pathname)

#     data = Future()
#     len = Future(location.nrows)
    
#     src(data, location)
#     val(len)

#     pt(data, Block())
#     pt(len, Replicated())

#     @partitioned data begin end

#     FutureDataFrame(data, len)
# end

# TODO: Implement reading/writing for S3FS and HTTP and for CSV, Parquet, Arrow

# compute samples as soon as possible
# delay the computation of sample properties
# transform and select produce new columns though transform keeps previous ones unless they are overwritten
# produced columns either use element-wise functions or they use functions that must be reduced
# 
# - columns
#   - which columns are potentially used for grouping
#   - when columns are kept but renamed without changing their divisions
#   - when columns are removed entirely
# - views

# function write_csv(df::FutureDataFrame, pathname)
#     location = CSVPath(pathname)
#     dst(df, location)
#     pt(df, Block())
#     mut(df)
#     partition_mut(df, :mut, Block(), CSVPath(pathname))
#     partition(df, Block(), )
#     @partitioned df begin end
# end

function read_csv(pathname; mount=nothing)
    df_loc = Remote(pathname, mount)
    df_nrows = Future(df_loc.nrows)
    DataFrame(Future(df_loc), df_size)
end

function write_csv(A, pathname; mount=nothing)
    df_loc = Remote(pathname, mount)
    dst(df, df_loc)
    partition(df, Replicated())
    partition(df, Distributed())
    @partitioned df begin end
    compute(df)
end

# TODO: Duplicate above functions for Parquet, Arrow

# DataFrame sampling

DataFrames.DataFrame <: AbstractSample

compute_size(df::DataFrames.DataFrame) = Base.summarysize()

# DataFrame properties

nrow(df::DataFrame) = compute(df.nrows)
ncol(df::DataFrame) = sample(df.size)[2]
size(df::DataFrame) = (nrow(df), ncol(df))
names(df::DataFrame, args...) = names(sample(df), args...)
propertynames(df::DataFrame) = propertynames(sample(df))

# DataFrame filtering

# grouping
# - group input on any key
# - group output with same key, divisions, different id
# - group input on any of a set of key (used for column selection), returning key that was used
# - group output with same key, divisions, same id or different if row selection
# - group input on any not in a set of key (used for column selection)
# - group output as unknown, same id or different if row selection
# Grouped(future, job) returns key, gpt, max_ngroups
# GroupedBy(future, key|keys, job) returns key, gpt, max_ngroups
# NotGroupedBy(future, key|keys, job) returns key, gpt, max_ngroups
# GroupedWith(future, key|keys, get_job())
# Balanced(dim=1)
# Distributing() for other cases
# Shuffled() for ID change

# function partition_for_filtering(df, res, res_nrows, args, kwargs)
#     partition(df, Replicated())
#     partition(res, Partitioned(); match_with=df) # TODO: Make Partitioned the default

#     partition(df, Blocked(;dim=1, balanced=true))
#     partition(df, Blocked(;dim=1, balanced=false))
#     # balanced is required because it determines what constraints are assigned
#     # if any w.r.t. MaxNPartitions, MemoryUsage
#     partition(res, Blocked(;dim=1, id='*', balanced=false))
#     # TODO: Make * in PTs both on client side and in pt_lib_info.json
#     # correspond to randomly generated Int32 IDs

#     partition_later() do job
#         for (key, gpt) in Grouped(df, job)
#             # We can't just constrain with distribution=:grouped because if
#             # this code region is the first in its stage and `df` was already
#             # partitioned with specific divisions in a previous stage, the
#             # scheduler wouldn't know that it can't re-split `df` with a
#             # different max_npartitions. So we must ensure the max_npartitions
#             # constraint is carried over here.
#             # TODO: Maybe introduce the concept of fixed constraints that are
#             # just always applied to certain values. This could be used to
#             # ensure a max_npartitions constraint is always applied
#             partition(df, gpt)
#             partition(
#                 res,
#                 Grouped(;key=key, id='*', balanced=false),
#                 match_with=df,
#                 match_on=:divisions
#             )
#         end
#     end

#     partition(res_nrows, Reducing(reducer=+))
#     partition(args, Replicated())
#     partition(kwargs, Replicated())
# end

# function futures_for_filtering(df, args, kwargs)
#     res_nrows = Future(df_nrows)
#     # There may still be skew in the data after filtering so we can't reduce
#     # the known memory usage of the result. We should mark the max_ngroups
#     # properties that were computed for keys as stale but this would mean that
#     # any subsequent GroupWith would result in new quantiles and hence, a
#     # shuffle. If the filtering was actually quite even, we wouldn't want to
#     # shuffle so we need a way to have GroupWith know what the previous
#     # quantiles are.
#     # TODO: Find a way so that re-splitting filtered data results in lower
#     # memory usage; maybe use same memory usage but make max_ngroups stale
#     res = DataFrame(Future(), res_nrows)
#     args = Future(args)
#     kwargs = Future(kwargs)

#     df, res, res_nrows, args, kwargs
# end

function dropmissing(df::DataFrame, args...; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of filtered dataframe"))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    args = Future(args)
    kwargs = Future(kwargs)

    partition(df, Replicated())
    partition(df, Blocked(dim=1, balanced=true))
    partition(df, Blocked(dim=1, balanced=false))
    union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    partition_later() do
        union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
        for key in sample(df, :allowedgroupingkeys)
            for balanced in [true, false]
                partition(df, Grouped(;key=key, balanced=balanced))
            end
        end
    end

    partition(res, Partitioned(balanced=false, id="*"), match_with=df, match_on=["distribution", "key", "divisions"])
    partition(res_nrows, Reducing(reducer=+))

    @partitioned df res res_nrows args kwargs begin
        res = dropmissing(df, args...; kwargs...)
        res_nrows = nrows(res)
    end

    res
end

function Base.filter(f, df::DataFrame; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of filtered dataframe"))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)

    partition(df, Replicated())
    partition(df, Blocked(dim=1, balanced=true))
    partition(df, Blocked(dim=1, balanced=false))
    union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    partition_later() do
        union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
        for key in sample(df, :allowedgroupingkeys)
            for balanced in [true, false]
                partition(df, Grouped(;key=key, balanced=balanced))
            end
        end
    end

    partition(res, Partitioned(balanced=false, id="*"), match_with=df, match_on=["distribution", "key", "divisions"])
    partition(res_nrows, Reducing(reducer=+))

    @partitioned df res res_nrows f begin
        res = filter(f, df)
        res_nrows = nrows(res)
    end

    res
end

# TODO: Make a `used` field and ensure that splitting/merging functions don't get used if their used are not provided

# DataFrame element-wise

function Base.copy(df::DataFrame)::DataFrame
    res = DataFrame(Future(), res_nrows)

    partition(df, Replicated())
    partition(df, Blocked(dim=1, balanced=true))
    partition(df, Blocked(dim=1, balanced=false))
    union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    setsample(res, :keystatistics, sample(df, :keystatistics))
    partition_later() do
        union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
        setsample(df, :keystatistics, sample(res, :keystatistics))
        for key in sample(df, :allowedgroupingkeys)
            for balanced in [true, false]
                partition(df, Grouped(;key=key, balanced=balanced))
            end
        end
    end

    partition(res, Partitioned(), match_with=res)
    mut(res)

    @partitioned df res begin
        res = copy(df)
    end

    DataFrame(res, copy(df.nrows))

    # df_nrows = df.size
    # res_nrows = Future(df_nrows) # This marks df_nrows as mutated if the value can't be copied over.
    # res_data = Future(;same_as=df) # None supports same_as (also copies memory_usage), same_keys_as, same_keys.
    # res = DataFrame(res_data, res_nrows)

    # # If some PAs have assignments for values which aren't assinged in
    # # other PAs, copy the assignment over to those previous PAs

    # partition(df, Replicated())
    # partition(df, Blocked(dim=1))
    # partition_later() do job
    #     # Grouped produces PTs for all different keys that df can be grouped on
    #     # and for both balanced and unbalanced with MaxNPartitions and
    #     # AbsoluteMemoryUsage constraints as needed.
    #     # The by/not_by (for selecting keys by arguments or other information
    #     # from the location of the annotated code) or like/not_like (for
    #     # selecting keys based on other data) options allow for constraining the
    #     # keys that can be used for grouping. A some_key_required option
    #     # indicates that one of the keys must be used for grouping. If set to
    #     # false (the default), it indicates that only a key required by some
    #     # _other_ code region must be used.
    #     for _, gpt in Grouped(df, job)
    #         partition(df, gpt)
    #     end
    # end

    # # We assign either RelativeMemoryUsage or AbsoluteMemoryUsage constraints
    # # or neither if the data is already unbalanced
    # partition(res, Partitioned(); match_with=df, mutating=true)
    # partition(df_nrows, ReplicatedOrReducing())
    # partition(res_nrows, Partitioned(), match_with=df_nrows)

    # # We don't mutate res_nrows since whether that needs to be mutated is
    # # determined by the Future constructor that is invoked above

    # @partitioned df df_nrows res res_nrows begin
    #     res = copy(df)
    #     res_nrows = df_nrows
    # end

    # res
end

# DataFrame column manipulation (with filtering)

# function getindex(df::DataFrame, rows, cols)
#     res_data = Future()
#     res_nrows = Future()
#     res = DataFrame(res_data, res_nrows)

#     # TODO: Add in rows and cols

#     partition(df, Replicated())
#     partition(res, Replicated())

#     partition_later() do job
#         selectivity = sample(length(res)) / sample(length(df))
#         partition(df, Distributing(;distribution=:blocked))
#         partition(res, Distributing(;distribution=:blocked, id='*', balanced=false), memory_usage_factor=1/selectivity)
#         # TODO: Figure out exactly how memory usage propagates especially for filters followed by filters

#         for (key, gpt, like) in GroupedLike(df, res)
#             # `gpt` is a PT with distribution=:grouped and key set to something
#             # that df may be grouped by in the future and divisions either
#             # computed using compute_quantiles or left unspecified with
#             # balanced=false
#             # 
#             # `GroupedLike` calls `compute_keys` on `res` and checks whether
#             # `df` is grouped by a key that `res` has or not
#             partition(df, gpt, max_npartitions=max_ngroups)
#             if like && rows isa Colon
#                 partition(res, Any(), match_with=df)
#             elseif like
#                 partition(
#                     res,
#                     Distributing(distribution=:grouped, key=key, id='*'),
#                     match_with=df,
#                     match_on=:divisions
#                 )
#             else
#                 partition(res, Distributing(distribution=:unknown, id='*'))
#             end
#         end
#     end

#     # TODO: Use partition for res_nrows

#     @partitioned df res res_nrows begin
#         res = getindex(df)
#         res_nrows = nrows(res)
#     end

#     res
# end

function getindex(df::DataFrame, rows, cols)
    # TODO: Accept either replicated, balanced, grouped by any of columns
    # in res (which is just cols of df if column selector is :), or unknown
    # and always with different ID unless row selector is :
    # and only allow : and copying columns without getting a view
    if !(rows isa Colon || rows isa Vector{Bool})
        throw(ArgumentError("Expected selection of all rows with : or some rows with Vector{Bool}"))
    end
    if cols == ! throw(ArgumentError("! is not allowed for selecting all columns; use : instead")) end

    # TODO: Remove this if not necessary
    if rows isa Colon && cols isa Colon
        return copy(df)
    end

    by = names(sample(df), cols)
    onecol = cols isa Symbol || cols isa String || cols isa Integer

    # TODO: Maybe make @partitioned be called first so that we can have error handling
    # get triggered first. But make sure that some stuff that needs to happen before
    # @partitioned happens like creating futures and using mut(future, new_future)

    # TODO: Handle case where cols is a single column by returning a Vector

    # TODO: Compute estimate of memory usage in code regions unless memory is
    # already specified to be something non-zero

    df_nrows = df.nrows
    res_nrows = rows isa Colon ? Future(df_nrows, onecol ? tuple : identity) : Future()
    res = onceol ? Vector(Future(), res_nrows) : DataFrame(Future(), res_nrows)
    rows = rows isa Vector ? rows : Future(rows)
    cols = Future(cols)

    partition(df, Replicated())
    partition(rows, Replicated())
    partition(res, Replicated())
    
    for balanced in [true, false]
        partition(df, Blocked(;dim=1, balanced=balanced))

        if rows isa Colon
            partition(rows, Replicated())
        elseif balanced
            partition(rows, Blocked(;dim=1, balanced=true))
        else
            partition(rows, Blocked(;dim=1); match_with=df, match_on="id")
        end

        if rows isa Colon
            partition(res, Blocked(;dim=1, balanced=balanced), match_with=df, match_on=["id"])
        else
            partition(res, Blocked(;dim=1, balanced=false, id="*"))
        end
    end

    # Merge sample properties of df to res
    # TODO: Make sample properies here work with onecol to produce array
    # TODO: Implement saving/loading functions for dataframe
    # TODO: Implement array operations, saving/loading for them
    if !onecol
        union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
        intersect!(sample(res, :allowedgroupingkeys), by)
        if rows isa Colon
            # Only merge column statistics if the column exists after filtering
            for key in by
                if key in sample(res, :names)
                    setsample(res, :keystatistics, key, sample(df, :keystatistics, key))
                end
            end
        end
    end

    partition_later() do
        if !onecol
            # Merge sample properties of res to df
            union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
        end

        # Distributed includes all Blocked and all Grouped on applicable keys
        # and both balanced and unbalanced
        # TODO: Have annotation code access and set job and sample properties
        # while PT constructor code is responsible for automatically
        # constructing constraints
        # TODO: Implement sample properties
        # - copying over from one future to another
        # - computing them
        # - renaming
        # - mutating
        for key in sample(df, :allowedgroupingkeys)
            for balanced in [true, false]
                # PT constructors serve to add in constraints needed to ensure
                # the parameters hold true. In other words, they make PTs
                # correct by construction
                partition(df, Grouped(;key=key, balanced=balanced))

                if rows isa Colon
                    partition(rows, Replicated())
                else
                    partition(rows, Blocked(;dim=1); match_with=df, match_on="id")
                end

                if !onecol && key in sample(res, :names)
                    if rows isa Colon
                        partition(res, Grouped(;key=key), match_with=df, match_on=["divisions", "balanced", "id"])
                    else
                        partition(res, Grouped(;key=key, balanced=false, id="*"), match_with=df, match_on="divisions")
                    end
                else
                    if rows isa Colon
                        partition(res, Blocked(;dim=1), match_with=df, match_on=["balanced", "id"])
                    else
                        # PT constructors should:
                        # - Create MaxNPartitions and MemoryUsage constraints if balanced
                        # - Create random ID if *
                        partition(res, Blocked(;dim=1, balanced=false, id="*"))
                    end
                end
            end
        end
    end

    partition(cols, Replicated())
    partition(df_nrows, ReplicatedOrReducing())
    if rows isa Colon
        partition(res_nrows, Reducing(;reducer=(onceol ? .+ : +)))
    else
        partition(res_nrows, ReplicatedOrReducing(match_with=df))
    end

    mut(res)

    @partitioned df df_nrows res res_nrows rows cols begin
        res = df[rows, cols]
        res_nrows = rows isa Colon ? df_nrows : length(res)
        res_nrows = rows isa Vector ? (rows) : rows
    end

    res

    # TODO: Solve same issue with skew for joins and filters
    # TODO: Make the total memory usage of a variable a constraint
    # - grouped -> account for skew
    # If no memory usage is specified, we use whatever the value had before
    # each task has default memory usage which is used if this is the first time
    # the value is being used; otherwise we switch only if the stage has a PA that specifies something
    # TODO: Require a full re-split to change total memory usage of a value
    # don't copy over quantiles or max_npartitions for filtering operations
    # - larger memory usage and same number of partitions
    # - smaller memory usage but smaller number of partitions
    # potential solution:
    # - reuse sample statistics for quantiles and max_ngroups as much as possible
    # - in all operations, handle each possible grouping key by computing quantiles and using appropriate max_ngroups all where skewed=false
    # - in all operations, automatically add in memoryusage constraints using sample statistics if o constraint is provided
    # - in the scheduler, save the memory usage of each value and only update if re-splitting EDIT: actually this isn't needed but instead we
    # should have the option to not default to memory usage from sample so that some PAs that, for example, require skewed=true won't rely on
    # memoryusage constraint or max_npartitions constraints being added in and instead just reuse from the previous partitioning
    # - in PT library, don't split or cast to skewed=true
    # - in all operations that filter or change distribution, return value with skewed=true and a memory usage that is relative to size of sample TODO
    # - in all operations, handle grouping where skewd=true by not setting a new max_ngroups or memoryusage
    # TODO join by using quantiles of either side of join (potentially resulting in skewed result) or the quantiles of the result but with
    # memory usage of inputs adjusted to account for skew in how they are expected to be partitioned to produce a balanced join result
    # TODO: Compute percentile spanned by quantiles associated with min max of join inputs in distribution of join output
    # TODO: Estimate memory usage of skewed output by multiplying outptut memory usage by selectivity for balanced and unknown and for grouped,
    # using percentiles of min amx of keys
    # TODO: Make partition accept parameters for adding in memoryusage constraints if they are needed (no_memory_usage, memory_usage_factor)
    # TODO: Make `evaluate` fill in uninitialized memoryusage constraints, so `partition` will either create MemoryUsage(some initial factor
    # to be multiplied with such as 1) or no constraint at all

    # TODO: MemoryUsage
    # - set memory usage of a variable to what is estimated by the sample if it is balanced
    # - otherwise set it to be relative to some other variable
    # - otherwise abstain and use what it already had in previous stage
    # options
    # - relative to optionally with skew
    # - equal to
    # - sampled
    # - no constraint
    # memory usage of each value has the following
    # - a base value that is either some absolute size or relative to one ore more values
    # - a scaling factor based on skew
    # We can achieve this by the following:
    # - keep track of memory usage of each variable in the backend
    # - for each potential fused PA for a stage, determine memory usage by looking at constraints

    # res_data = cols isa Colon ? Future(;same_keys_as=df, same_memory_usage_as=df) : Future()
    # res_data = Future()

    # TODO: Maybe make some way to mutate the value according to the later
    # approximation of cols
    # TODO: Store length instead of size for DataFrame
    # res_nrows = rows isa Colon ? Future(df.size, ???) : Future()

    # res_nrows = Future()
    # res = DataFrame(res_data, res_nrows)

    # partition(df, Replicating())
    # partition(res, Replicating(), match_with=df)

    # partition(df, Balanced(dim=1))
    # partition(df, Distributing(distribution=:unknown))
    # if rows isa Colon
    #     partition(res, Any(); match_with=df, match_on="id")
    # else:
    #     partition(res, Distributing(distribution=:unknown, id='*'))
    # end

    # None location constructor - determines sample-related stuff

    # find min-max of filtered
    # find quantile percent spanned by min max range 
    # take the inverse of the percentage and multiply by mem. usage
    # TODO: On filtering operations, group by quantiles to get 
    # - have constraint that specifies adjustment to memory usage
    # - on filtering, look at quantiles to determine adjustment

    # Make compute_divisions have option to check for divisons and only update if
    # the difference is too much
    # TODO: Figure out relationship between filtering and divisions and memory usage

    # partition_later() do job
    #     # TODO: Maybe seperate into PT constructor and constraint constructor for AtMost
    #     for (key, like, gpt, max_ngroups) in GroupedLike(df, res, job)
    #         partition(df, gpt, max_npartitions=max_ngroups)
    #         if like && rows isa Colon
    #             partition(res, Any(), match_with=df)
    #         elseif like
    #             partition(
    #                 res,
    #                 Distributing(distribution=:grouped, key=key, id='*'),
    #                 match_with=df,
    #                 match_on=:divisions
    #             )
    #         else
    #             partition(res, Distributing(distribution=:unknown, id='*'))
    #         end
    #     end
    # end

    # partition(res_nrows, rows isa Colon ? Replicated() : Reduce(dim=1))

    # TODO: Make mut accept multiple arguments
    # TODO: Implement partition

    # mut(res, res_nrows)
    # @partitioned df res res_nrows rows cols begin
    #     res = getindex(df, rows)
    #     res_nrows = size(res)
    # end
end

function setindex!(df::DataFrame, v::Union{Vector, Matrix, DataFrame}, rows, cols)
    rows isa Colon || throw(ArgumentError("Cannot mutate a subset of rows in place"))

    selection = names(sample(df), cols)

    res = Future()
    cols = Future(cols)

    union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    # Only merge column statistics if the column is not mutated by setindex!
    for key in sample(df, :names)
        if !(key in selection)
            setsample(res, :keystatistics, key, sample(df, :keystatistics, key))
        end
    end

    partition(df, Replicated())
    partition(col, Replicated())
    
    partition(df, Blocked(dim=1, balanced=true))
    partition(v, Blocked(dim=1, balanced=true))
    
    partition(df, Blocked(dim=1, balanced=false))
    partition(res, Partitioned(); match_with=df)

    # TODO: Implement partition_later to end the current PA and store
    # function that can potentially (but may also not) create a new PA

    partition_later() do
        for key in sample(res, :allowedgroupingkeys)
            if key in sample(df, :names)
                push!(sample(res, :allowedgroupingkeys))
            end
        end
        for key in sample(res, :names)
            if !(key in selection)
                setsample(df, :keystatistics, key, sample(res, :keystatistics, key))
            end
        end

        for key in sample(df, :allowedgroupingkeys)
            for balanced in [true, false]
                partition(df, Grouped(;key=key, balanced=balanced))
                if key in selection
                    partition(res, Blocked(dim=1, balanced=balanced); match_with=df, match_on="id")
                else
                    partition(res, Partitioned(); match_with=df)
                end
            end
        end 
    end

    partition(v, Blocked(dim=1, balanced=false), match_with=df, match_on="id")
    partition(cols, Replicated())

    # res_data = cols isa Colon ? Future()
    # res_nrows = rows isa Colon ? Future(df.size) : Future()
    # res = DataFrame(res_data, res_nrows)

    # mut will replace df with the future and sample and location of res and
    # record a DestroyRequest on the original future in df. The contents of
    # res are modified to ensure that when its destructor is called a
    # DestroyRequest is not called on the new future which is now stored in df.
    # This could be done by setting the value_id to nothing and having the
    # finalizer of Future only record a DestroyRequest if the value ID isn't
    # set to nothing. This should also indicate that a sample should not be
    # collected for the thing being mutated.
    # TODO: Implement this version of mut
    mut(df, res)

    @partition df col cols res begin
        df[:, cols] = col
        res = df
    end

    # partition()  begin job
    #     for gpt, max_ngroups in GroupedBy(df, )
    #         partition()
    #     end
    # end
end

# function Base.getproperty(df::DataFrame, cols)
# end

# function Base.setproperty!(df::DataFrame, cols)
#     # Groups on any of columns of df but resulting df is not grouped on anything that is in the columns
#     # being set
#     # 2 cases
#     # - any of the columns in the input DF - GroupedBy (equivalents for Balanced for arrays)
#     # - any of the columns in the args - GroupedBy
#     # - any of the columns not in the args but in the input DF - NotGroupedBy
#     # - any of the columns in the args but actually use quantile - GroupedWith
#     # 3 cases
#     # - compute atmost and quantiles
#     # - compute atmost
#     # - don't compute anything
#     # define functions for each sample type
#     # - compute_keys
#     # - compute_axes
#     # - compute_max_ngroups
#     # - compute_quantiles
#     # define PT constructors that accept only a Future and columns to group on
#     # Distributed(distribution, key, divisions, id)
#     # - GroupedBy(compute_divisions=true)
#     # - GroupedBy(compute_divisions=true, compute_max_ngroups=true)
#     allcols = hash.(df |> approximate |> eachcol |> keys)
#     partition() begin job
#         for gpt, max_ngroups in GroupedBy(df, )
#             partition()
#         end
#     end
#     # TODO: Accept either replicated, balanced, or same ID unless rows are selected grouped by any of columns
#     # in res (which is just cols of df if column selector is :), or unknown
#     # and always with different ID unless row selector is :
#     # and only allow : and copying columns without getting a view
#     # TODO: Make the Location just produce a sample and then have this sample used
#     # to produce the total memory usage; effectively have the sample translated into
#     # total memory usage when compiled
#     # TODO: Make sure that in generated code on client side, mutated values
#     # have their samples copied before hand

#     # Several PT constructors that may be useful
#     # BlockAndBalanced(keys)
#     # BlockAndUnbalanced(keys)
#     # GroupedAndBalanced(keys) -> Distributing...
#     # GroupedAndUnbalanced(keys) -- key, gpt with constraints

#     # General:
#     # - Replicated
#     # - Blocked by some or all key(s)
#     # - Grouped by some or all key(s)
#     # Considerations:
#     # - copying over column statistics
#     # - partition both data and length
#     # - Balanced/Unbalanced
#     # - MemoryUsage, MaxNPartitions, Match, MatchOn
#     # - partition now or later
#     # - mutating

#     # The important constraints that are produced in PT constructors or in
#     # `partition` are MaxNPartitions and MemoryUsage

#     # MemoryUsage has three options
#     # 1. Specific memory usage that this value is guaranteed to have
#     # 2. Memory usage relative to some other value by some factor (typically based on selectivity, samples, min-max of keys and quantiles)
#     # 3. No constraint if it is required for something to be unbalanced (if no constraints are applied, the most recent size is used in scheduler)
# end

function rename(df::DataFrame, args...; kwargs...)
    # TODO: Make partition_delayed calls be processed in reverse
    # TODO: Populate groupingkeys of samples forwards and backwards
    # TODO: Copy over sample properties forwards and backwards

    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partition(df, Replicated())
    partition(res, Replicated())
    
    for balanced in [true, false]
        partition(df, Blocked(;dim=1, balanced=balanced))
        partition(res, Blocked(;dim=1, balanced=balanced), match_with=df, match_on=["id"])
    end

    partition_later() do
        allowedgroupingkeys = sample(res, :allowedgroupingkeys)
        statistics = sample(res, :keystatistics, key)
        for (i, key) in enumerate(sample(res, :names))
            prevkey = sample(df, :names)[i]
            if key in keys(statistics)
                setsample(df, :keystatistics, prevkey, statistics)
            end
            if key in allowedgroupingkeys
                push!(sample(df, :allowedgroupingkeys), prevkey)
                for balanced in [true, false]
                    partition(df, Grouped(;key=prevkey, balanced=balanced))
                    partition(res, Grouped(;key=key), match_with=df, match_on=["divisions", "balanced", "id"])
                end
            end
        end
    end

    # for k in keys(d)
    #     gpt, max_ngroups = GroupedBy(df; key=k)
    #     partition(df, gpt, at_most=max_ngroups)
    #     partition(df, Grouped(res; key=k), match_with=df, match_on=[:divisions, :id])
    # end
    # partition() do job
    #     for (gpt, max_ngroups) in GroupedBy(df, stalekeys)
    #         partition(df, gpt, at_most=max_ngroups)
    #         partition(res)
    #     end
    # end

    # res = DataFrame()
    # partition(df, GroupedBy())
    # partition(res, GroupedBy(), match_with=df, match_on=["distribution", "axis", "divisions"])
    # res

    mut(res)

    @partitioned df res args kwargs begin
        res = rename(df, args...; kwargs...)
    end

    allowedgroupingkeys = sample(df, :allowedgroupingkeys)
    statistics = sample(df, :keystatistics, key)
    for (i, key) in enumerate(sample(df, :names))
        newkey = sample(res, :names)[i]
        if key in allowedgroupingkeys
            push!(sample(res, :allowedgroupingkeys), newkey)
        end
        if key in keys(statistics)
            setsample(res, :keystatistics, newkey, statistics[key])
        end
    end

    DataFrame(res, copy(df.nrows))
end

# TODO: Implement select/transform/combine for DataFrame
# TODO: In rename/select/transform/combine figure out what columns to copy over statistics for
# - compute grouping column, owned columns, mutated columns and pass these to PT constructors so that
# they can filter to the relevant subset
# - ensure that we don't reuse statistics for keys that are mutated or assigned to, basically taking into
# account that the key sample properties might be there for keys that aren't actually present in the DF

# function select(df::DataFrame, args...; kwargs...)
#     !get(kwargs, :copycols, false) || throw(ArgumentError("Cannot return view of selected dataframe"))

#     for arg in args
#         source = arg isa Pair
#     end

#     res = Future()

#     # same statistics only if the column is not in the results of a transformation

#     @partitioned df args kwargs begin
        
#     end

#     DataFrame(res, copy(df.nrows))
# end

# function transform(df::DataFrame, args...; kwargs...)
#     !get(kwargs, :copycols, false) || throw(ArgumentError("Cannot return view of transformed dataframe"))
# end

# function combine(df::DataFrame, args...; kwargs...)
# end

# DataFrame shuffling

function sort(df::DataFrame, cols; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of sorted dataframe"))

    # Determine what to sort by and whether to sort in reverse
    firstcol = (cols isa Vector ? first(cols) || cols)
    by, isreversed = if firstcol isa DataFrames.UserColOrdering
        if isempty(firstcol.kwargs) || (length(firstcol.kwargs) == 1 && haskey(firstcol.kwargs, :rev))
            string(firstcol.col), get(firstcol.kwargs, :rev, false)
        else
            throw(ArgumentError("Only rev is supported for ordering"))
        end
    else
        first(names(sample(df), firstcol)), false
    end
    isreversed = get(kwargs, :rev, false) || isreversed

    res = Future()
    cols = Future(cols)
    kwargs = Future(kwargs)

    # TODO: Only allow first column to have rev

    # TODO: Only use at most first 8 for allowedgroupingkeys
    by = first(names(sample(df), cols))
    push!(sample(df, :allowedgroupingkeys), by)
    push!(sample(res, :allowedgroupingkeys), by)

    # We must construct seperate PTs for balanced=true and balanced=false
    # because these different PTs have different required constraints
    # TODO: Implement reversed in Grouped constructor
    partition(df, Grouped(;key=by, balanced=true, reversed=isreversed))
    partition(df, Grouped(;key=by, balanced=false, reversed=isreversed))
    partition(res, Grouped(;key=by, balanced=false, id="*"), match_with=df, match_on=["divisions"])
    partition(cols, Replicated())
    partition(kwargs, Replicated())

    mut(res)

    @partitioned df res res_nrows cols kwargs begin
        res = sort(cols; kwargs...)
    end

    statistics = sample(df, :keystatistics, by)
    if by in keys(statistics)
        setsample(res, :keystatistics, by, statistics[by])
    end

    DataFrame(res, copy(df.nrows))
end

function innerjoin(df1::DataFrame, df2::DataFrame, args...; on, kwargs...)
    isempty(args) || throw(ArgumentError("Join of multiple dataframes is not supported"))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    kwargs = Future(kwargs)

    partition(df1, Replicated())
    partition(df2, Replicated())
    partition(res, Replicated())
    partition(res_nrows, Replicated())

    for balanced in [true, false]
        partition(df1, Replicated())
        partition(df2, Replicated())
        partition(res, Blocked())
    end

    # TODO: Support broadcast join
    # TODO: Implement proper join support where different parties are used for
    # determining the distribution

    on = [k isa Pair ? k : (k => k) for k in (on isa Vector ? on : [on])]
    on_left = first.(on)
    union!(sample(res, :allowedgroupingkeys), on_left)
    union!(sample(df1, :allowedgroupingkeys), on_left)
    union!(sample(df2, :allowedgroupingkeys), last.(on))
    # No statistics are copied over because joins are selective and previous
    # statistics would no longer apply
    for key in on
        for balanced in [true, false]
            partition(df1, Grouped(;key=first(key), balanced=balanced))
            partition(df2, Grouped(;key=last(key), balanced=balanced))
            partition(res, Grouped(;balanced=false, id="*"), match_with=df1, match_on=["key", "divisions"])
        end
    end

    partition(res_nrows, Reducing(;reducer=+))
    partition(kwargs, Replicated())

    mut(res)
    mut(res_nrows)

    @partitioned df1 df2 res res_nrows kwargs begin
        res = innerjoin(df1, df2; kwargs...)
        res_nrows = nrows(res)
    end

    res
end

function unique(df::DataFrame, cols=nothing; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of Banyan dataframe"))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    cols = Future(cols)

    partition(df, Replicated())
    partition(res, Replicated())
    partition(res_nrows, Replicated())

    # TODO: Only use at most first 8 for allowedgroupingkeys
    by = isnothing(cols) ? sample(df, :names) : names(sample(df), cols)
    union!(sample(df, :allowedgroupingkeys), by)
    union!(sample(res, :allowedgroupingkeys), by)
    for key in by
        for balanced in [true, false]
            partition(df, Grouped(;key=key, balanced=balanced))
        end
    end

    partition(res, Grouped(;balanced=false, id="*"), match_with=df, match_on=["key", "divisions"])
    partition(res_nrows, Reducing(;reducer=+))
    partition(cols, Replicated())

    mut(res)
    mut(res_nrows)

    @partitioned df res res_nrows cols begin
        res = isnothing(cols) ? unique(df) : unique(df, cols)
        res_nrows = nrows(res)
    end

    res
end

function nonunique(df::DataFrame, cols=nothing; kwargs...)
    if get(kwargs, :view, false) throw(ArgumentError("Cannot return view of Banyan dataframe")) end

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    cols = Future(cols)

    partition(df, Replicated())
    partition(res, Replicated())
    partition(res_nrows, Replicated())

    by = isnothing(cols) ? sample(df, :names) : names(sample(df), cols)
    union!(sample(df, :allowedgroupingkeys), by)
    union!(sample(res, :allowedgroupingkeys), by)
    for key in by
        for balanced in [true, false]
            partition(df, Grouped(;key=key, balanced=balanced))
        end
    end

    partition(res, Grouped(;balanced=false, id="*"), match_with=df, match_on=["key", "divisions"])
    partition(res_nrows, Reducing(;reducer=+))
    partition(cols, Replicated())

    # TODO: Maybe make mut automatic or re-evaluate exactly what it is used for
    mut(res)
    mut(res_nrows)

    @partitioned df res res_nrows cols begin
        res = isnothing(cols) ? nonunique(df) : nonunique(df, cols)
        res_nrows = nrows(res)
    end

    res
end

# TODO: Implement SubDataFrame

struct GroupedDataFrame <: AbstractFuture
    data::Future
    length::Future
    parent::DataFrame
    groupcols::Future
    groupkwargs::Future

    # GroupedDataFrame() = new(Future(), Future(), Future())
    # GroupedDataFrame(gdf::GroupedDataFrame) =
    #     new(Future(), Future(gdf.nrows), Future(gdf.offset))
end

convert(::Type{Future}, gdf::GroupedDataFrame) = gdf.data

length(gdf::GroupedDataFrame) = compute(gdf.nrows)
groupcols(gdf::GroupedDataFrame) = groupcols(sample(gdf))
valuecols(gdf::GroupedDataFrame) = valuecols(sample(gdf))

# GroupedDataFrame creation

function groupby(df::DataFrame, cols; kwargs...)::GroupedDataFrame
    gdf_data = Future()
    gdf_length = Future()
    cols = Future(cols)
    kwargs = Future(kwargs)
    gdf = GroupedDataFrame(Future(), gdf_length, df, cols, kwargs)

    partition(df, Replicated())
    partition(gdf, Replicated())
    partition(gdf_length, Replicated())

    allowedgroupingkeys = names(sample(df), compute(cols))
    allowedgroupingkeys = get(kwargs, :sort, false) ? allowedgroupingkeys[1:1] : allowedgroupingkeys
    union!(sample(df, :allowedgroupingkeys), allowedgroupingkeys)
    setsample(gdf, :allowedgroupingkeys, allowedgroupingkeys)
    for key in allowedgroupingkeys
        for balanced in [true, false]
            partition(df, Grouped(;key=key, balanced=balanced))
        end
        # Grouped computes keystatistics for key for df
        setsample(gdf, :keystatistics, key, sample(df, :keystatistics, key))
    end

    partition(gdf, Blocked(;dim=1), match_with=df, match_on=["balanced", "id"])
    partition(gdf_length, Reducing(;reducer=+))
    partition(cols, Replicated())
    partition(kwargs, Replicated())
    # TODO: Ensure splitting/merging functions work for Blocked on GroupedDataFrame

    mut(gdf)
    mut(gdf_length)

    @partitioned df gdf gdf_length cols kwargs begin
        gdf = groupby(df, cols; kwargs...)
        gdf_length = length(gdf)
    end
    
    gdf

    # TODO: approximate -> sample and evaluate -> compute

    # w.r.t. keys and axes, there are several things you need to know:
    # - reuse of columns 
    # Create Future for result

    # gdf = GroupedDataFrame()
    # gdf_len = gdf.whole_len
    # df_len = df.whole_len
    # for (gpt, max_ngroups) in Grouped(gdf, )
    # partition(gdf, Distributed(), parent=df)
    # @partitioned df gdf begin end

    # when merging a GroupedDataFrame which must be pseudogrouped,
    # vcat the parents and the groupindices and modify the cat-ed parents
    # to have a column for the parent index and the gorup iindex within that parent
    # and then do a group-by on this
    # for writing to disk, just be sure to put everything into a dataframe such that it
    # can be read back and have a column that specifies how to group by
end

# GroupedDataFrame column manipulation

function select(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partition(gdf, Replicated())
    partition(gdf_parent, Replicated())
    partition(res, Replicated())

    # TODO: Share sampled names if performance is impacted by repeatedly getting names

    # allowedgroupingkeys = names(sample(gdf_parent), compute(groupcols))
    # allowedgroupingkeys = get(compute(groupkwargs), :sort, false) ? allowedgroupingkeys[1:1] : allowedgroupingkeys
    # union!(sample(gdf_parent, :allowedgroupingkeys), allowedgroupingkeys)
    if get(compute(kwargs), :keepkeys, true)
        union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    end
    for key in sample(gdf_parent, :allowedgroupingkeys)
        setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
        for balanced in [true, false]
            partition(gdf_parent, Grouped(;key=key, balanced=balanced))
            if get(compute(kwargs), :keepkeys, true)
                partition(res, Partitioned(), match_with=gdf_parent)
            else
                partition(res, Blocked(dim=1), match_with=gdf_parent, match_on=["balanced", "id"])
            end
        end
    end
    partition(gdf, Blocked(;dim=1), match_with=gdf_parent, match_on=["balanced", "id"])

    partition(groupcols, Replicated())
    partition(groupkwargs, Replicated())
    partition(args, Replicated())
    partition(kwargs, Replicated())

    # if kwargs[:ungroup]

    # else
    #     res = GroupedDataFrame(gdf)
    #     res_nrows = res.nrows
    #     partition(gdf, Pseudogrouped())
    #     partition(args, Replicated())
    #     partition(kwargs, Replicated())
    #     @partitioned gdf res res_nrows args kwargs begin
    #         res = select(gdf, args..., kwargs...)
    #         res_nrows = length(gdf_nrows)
    #     end
    # end

    mut(res)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = select(gdf, args...; kwargs...)
    end
end

function transform(gdf::GroupedDataFrame)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partition(gdf, Replicated())
    partition(gdf_parent, Replicated())
    partition(res, Replicated())
    
    if get(compute(kwargs), :keepkeys, true)
        union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    end
    for key in sample(gdf_parent, :allowedgroupingkeys)
        setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
        for balanced in [true, false]
            partition(gdf_parent, Grouped(;key=key, balanced=balanced))
            if get(compute(kwargs), :keepkeys, true)
                partition(res, Partitioned(), match_with=gdf_parent)
            else
                partition(res, Blocked(dim=1), match_with=gdf_parent, match_on=["balanced", "id"])
            end
        end
    end
    partition(gdf, Blocked(;dim=1), match_with=gdf_parent, match_on=["balanced", "id"])

    partition(groupcols, Replicated())
    partition(groupkwargs, Replicated())
    partition(args, Replicated())
    partition(kwargs, Replicated())

    mut(res)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = transform(gdf, args...; kwargs...)
    end

    res
end

function combine(gdf::GroupedDataFrame)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partition(gdf, Replicated())
    partition(gdf_parent, Replicated())
    partition(res, Replicated())
    
    if get(compute(kwargs), :keepkeys, true)
        union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    end
    for key in sample(gdf_parent, :allowedgroupingkeys)
        for balanced in [true, false]
            partition(gdf_parent, Grouped(;key=key, balanced=balanced))
            if get(compute(kwargs), :keepkeys, true)
                partition(res, Grouped(key=key, balanced=false, id="*"), match_with=gdf_parent, match_on="divisions")
            else
                partition(res, Blocked(dim=1, balanced=false, id="*"))
            end
        end
    end
    partition(gdf, Blocked(;dim=1), match_with=gdf_parent, match_on=["balanced", "id"])

    partition(groupcols, Replicated())
    partition(groupkwargs, Replicated())
    partition(args, Replicated())
    partition(kwargs, Replicated())

    # TODO: Allow for putting multiple variables that share a PT in a call to partition

    mut(res)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = combine(gdf, args...; kwargs...)
    end

    res
end

# TODO: Implement filter using some framework for having references by keeping
# track of the lineage of which code regions produced which 

function subset(gdf::GroupedDataFrame)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partition(gdf, Replicated())
    partition(gdf_parent, Replicated())
    partition(res, Replicated())
    
    if get(compute(kwargs), :keepkeys, true)
        union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    end
    for key in sample(gdf_parent, :allowedgroupingkeys)
        for balanced in [true, false]
            partition(gdf_parent, Grouped(;key=key, balanced=balanced))
            if get(compute(kwargs), :keepkeys, true)
                partition(res, Grouped(key=key, balanced=false, id="*"), match_with=gdf_parent, match_on="divisions")
            else
                partition(res, Blocked(dim=1, balanced=false, id="*"))
            end
        end
    end
    partition(gdf, Blocked(;dim=1), match_with=gdf_parent, match_on=["balanced", "id"])

    partition(groupcols, Replicated())
    partition(groupkwargs, Replicated())
    partition(args, Replicated())
    partition(kwargs, Replicated())

    mut(res)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = subset(gdf, args...; kwargs...)
    end

    res
end

function run_iris()
    # TODO: Read in iris
    # TODO: Conver petal length units
    # TODO: Average the petal length
    # df = read_csv("s3://banyanexecutor/iris.csv")
    df = read_csv("s3://banyan-cluster-data-mycluster/datasets/input/")
    write_csv(df, "s3://banyan-cluster-data-mycluster/datasets/output/")
    println(compute(df.len))

    compute(df)
end

@testset "Iris" begin
    run_with_job("Iris", j -> begin
        run_iris()
    end)
end
