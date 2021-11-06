struct DataFrame <: AbstractFuture
    data::Future
    nrows::Future
    # TODO: Add offset for indexing
    # offset::Future
end

Banyan.convert(::Type{Future}, df::DataFrame) = df.data

# DataFrame creation

# function read_csv(pathname)
#     location = CSVPath(pathname)

#     data = Future()
#     len = Future(location.nrows)
    
#     sourced(data, location)
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
#     destined(df, location)
#     pt(df, Block())
#     mutated(df)
#     partition_mutated(df, :mut, Block(), CSVPath(pathname))
#     partition(df, Block(), )
#     @partitioned df begin end
# end

function read_csv(path::String; kwargs...)
    df_loc = RemoteSource(path; kwargs...)
    df_loc.src_name == "Remote" || error("$path does not exist")
    df_nrows = Future(df_loc.nrows)
    DataFrame(Future(source=df_loc), df_nrows)
end

read_parquet(p; kwargs...) = read_csv(p; kwargs...)
read_arrow(p; kwargs...) = read_csv(p; kwargs...)

# TODO: For writing functions, if a file is specified, enforce Replicated

function write_csv(df, path; invalidate_source=true, invalidate_sample=true, kwargs...)
    # destined(df, Remote(path, delete_from_cache=true))
    # mutated(df)
    # partitioned_with() do
    #     pt(df, Partitioned(df))
    # end
    # @partitioned df begin end
    # compute(df)
    # sourced(df, Remote(path)) # Allow data to be read from this path if needed in the future
    # destined(df, None())
    partitioned_with() do
        pt(df, Partitioned(df))
    end
    partitioned_computation(
        df,
        destination=RemoteDestination(path; invalidate_source=invalidate_source, invalidate_sample=invalidate_sample, kwargs...),
        new_source=_->RemoteSource(path)
    )
end

write_parquet(A, p; kwargs...) = write_csv(A, p; kwargs...)
write_arrow(A, p; kwargs...) = write_csv(A, p; kwargs...)

function Banyan.write_to_disk(df::DataFrame)
    partitioned_with() do
        pt(df, Partitioned(df))
    end
    partitioned_computation(df, destination=Disk())
end

# TODO: Duplicate above functions for Parquet, Arrow

# DataFrame sample

Banyan.sample_axes(df::DataFrames.DataFrame) = [1]
Banyan.sample_keys(df::DataFrames.DataFrame) = propertynames(df)

# TODO: Make these sample_* functions handle empty data frames

function Banyan.sample_divisions(df::DataFrames.DataFrame, key)
    # There are no divisions for empty data
    if isempty(df)
        return []
    end

    max_ngroups = sample_max_ngroups(df, key)
    ngroups = min(max_ngroups, 512)
    data = sort(map(orderinghash, df[!, key]))
    datalength = length(data)
    grouplength = div(datalength, ngroups)
    # We use `unique` here because if the divisions have duplicates, this could
    # result in different partitions getting the same divisions. The usage of
    # `unique` here is more of a safety precaution. The # of divisions we use
    # is the maximum # of groups.
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

function Banyan.sample_percentile(df::DataFrames.DataFrame, key, minvalue, maxvalue)
    # If the data frame is empty, nothing between `minvalue` and `maxvalue` can
    # exist in `df`. so the percentile is 0.
    if isempty(df) || isnothing(minvalue) || isnothing(maxvalue)
        return 0
    end

    # NOTE: This may cause some problems because the way that data is ultimately split may
    # not allow a really fine division of groups. So in the case of some filtering, the rate
    # of filtering may be 90% but if there are only like 3 groups then maybe it ends up being like
    # 50% and that doesn't get scheduled properly. We can try to catch stuff like maybe by using
    # only 70% of total memory in scheduling or more pointedly by changing this function to
    # call sample_divisions with a reasonable number of divisions and then counting how many
    # divisions the range actually belongs to.

    count(map(o -> begin oh = orderinghash(o); oh >= minvalue && oh <= maxvalue end, df[!, key])) / nrow(df)

    # # minvalue and maxvalue should already be order-preserved hashes
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

Banyan.sample_max_ngroups(df::DataFrames.DataFrame, key) = isempty(df) ? 0 : div(nrow(df), maximum(combine(groupby(df[!, [key,]], key), nrow).nrow))
Banyan.sample_min(df::DataFrames.DataFrame, key) = isempty(df) ? nothing : minimum(map(orderinghash, df[!, key]))
Banyan.sample_max(df::DataFrames.DataFrame, key) = isempty(df) ? nothing : maximum(map(orderinghash, df[!, key]))

# DataFrame properties

DataFrames.nrow(df::DataFrame) = collect(df.nrows)
DataFrames.ncol(df::DataFrame) = ncol(sample(df))
Base.size(df::DataFrame) = (nrow(df), ncol(df))
Base.ndims(df::DataFrame) = 2
Base.names(df::DataFrame, args...) = names(sample(df), args...)
Base.propertynames(df::DataFrame) = propertynames(sample(df))

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
#     partition(res, Partitioned(); match=df) # TODO: Make Partitioned the default

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
#                 match=df,
#                 on=:divisions
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

function pts_for_filtering(init::AbstractFuture, final::AbstractFuture; with, kwargs...)
    # for (initpt, finalpt) in zip(
    #     with(init; balanced=false, filtered_to=final, kwargs...),
    #     with(final; balanced=false, filtered_from=init, kwargs...),
    # )
    #     # unbalanced -> balanced
    #     pt(init, initpt, match=final, on=["distribution", "key", "divisions", "rev"])
    #     pt(final, Balanced() & Drifted())

    #     # unbalanced -> unbalanced
    #     pt(init, initpt, match=final, on=["distribution", "key", "divisions", "rev"])
    #     pt(final, finalpt & Drifted())

    #     # balanced -> unbalanced
    #     pt(init, Balanced(), match=final, on=["distribution", "key", "divisions", "rev"])
    #     pt(final, finalpt & Drifted())
    # end
    # Initially, we thought that the above was okay (not computing the
    # divisions by calling `with` with `balanced=true`). But then we realized
    # that you might have some blocked data that you then need to call `unique`
    # on and so you need to group it and then you're going to `comput` it right
    # afterwards. In this scenario, you need to have a PT where you compute
    # divisions with a call to `with` where `balanced=true`.
    for (initpt_unbalanced, finalpt_unbalanced, initpt_balanced, finalpt_balanced) in zip(
        # There should be a balanced and unbalanced PT for each possible key
        # that the initial/final data can be grouped on
        # unbalanced
        with(init; balanced=false, filtered_to=final, kwargs...),
        with(final; balanced=false, filtered_from=init, kwargs...),
        # balanced
        with(init; balanced=true, filtered_to=final, kwargs...),
        with(final; balanced=true, filtered_from=init, kwargs...),
    )
        # unbalanced -> balanced
        pt(init, initpt_unbalanced, match=final, on="divisions")
        pt(final, finalpt_balanced & Drifted())

        # unbalanced -> unbalanced
        pt(init, initpt_unbalanced, match=final, on=["distribution", "key", "divisions", "rev"])
        pt(final, finalpt_unbalanced & Drifted())

        # balanced -> unbalanced
        pt(init, initpt_balanced, match=final, on="divisions")
        pt(final, finalpt_unbalanced & Drifted())
    end
end

function DataFrames.dropmissing(df::DataFrame, args...; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of filtered dataframe"))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    args = Future(args)
    kwargs = Future(kwargs)

    # partition(df, Replicated())
    # partition(df, Blocked(dim=1, balanced=true))
    # partition(df, Blocked(dim=1, balanced=false))
    # union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    # partition_later() do
    #     union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
    #     for key in sample(df, :allowedgroupingkeys)
    #         for balanced in [true, false]
    #             partition(df, Grouped(;key=key, balanced=balanced))
    #         end
    #     end
    # end

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        # We need to maintain these sample properties and hold constraints on
        # memory usage so that we can properly handle data skew
        keep_all_sample_keys(res, df, drifted=true)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pts_for_filtering(df, res, with=Distributed)
        pt(res_nrows, Reducing(quote (a, b) -> a .+ b end))
        pt(df, res, res_nrows, args, kwargs, Replicated())
    end

    # partition(res, Partitioned(balanced=false, id="*"), match=df, on=["distribution", "key", "divisions"])
    # partition(res_nrows, Reducing(reducer=+))

    @partitioned df res res_nrows args kwargs begin
        res = DataFrames.dropmissing(df, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    res
end

function Base.filter(f, df::DataFrame; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of filtered dataframe"))

    f = Future(f)
    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    kwargs = Future(kwargs)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df, drifted=true)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pts_for_filtering(df, res, with=Distributed)
        pt(res_nrows, Reducing(quote (a, b) -> a .+ b end))
        pt(df, res, res_nrows, f, kwargs, Replicated())
    end

    @partitioned df res res_nrows f kwargs begin
        # @show df
        res = @time DataFrames.filter(f, df; kwargs...)
        # @show res
        res_nrows = DataFrames.nrow(res)
        # @show res_nrows
    end

    res
end

# TODO: Make a `used` field and ensure that splitting/merging functions don't get used if their used are not provided

# DataFrame element-wise

function Missings.allowmissing(df::DataFrame)::DataFrame
    res = Future()

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pt(df, Distributed(df, scaled_by_same_as=res))
        pt(res, ScaledBySame(as=df), match=df)

        # pt(df, Distributed(df, balanced=true))
        # pt(res, Balanced(), match=df)

        # pt(df, Distributed(df, balanced=false, scaled_by_same_as=res))
        # pt(res, Unbalanced(scaled_by_same_as=df), match=df)
        
        pt(df, res, Replicated())
    end

    @partitioned df res begin res = DataFrames.allowmissing(df) end

    DataFrame(res, copy(df.nrows))
end

function Missings.disallowmissing(df::DataFrame)::DataFrame
    res = Future()

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pt(df, Distributed(df, scaled_by_same_as=res))
        pt(res, ScaledBySame(as=df), match=df)

        # pt(df, Distributed(df, balanced=true))
        # pt(res, Balanced(), match=df)

        # pt(df, Distributed(df, balanced=false, scaled_by_same_as=res))
        # pt(res, Unbalanced(scaled_by_same_as=df), match=df)
        
        pt(df, res, Replicated())
    end

    @partitioned df res begin res = DataFrames.disallowmissing(df) end

    DataFrame(res, copy(df.nrows))
end

function Base.deepcopy(df::DataFrame)::DataFrame
    res = Future()

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pt(df, Distributed(df, scaled_by_same_as=res))
        pt(res, ScaledBySame(as=df), match=df)

        # pt(df, Distributed(df, balanced=true))
        # pt(res, Balanced(), match=df)

        # pt(df, Distributed(df, balanced=false, scaled_by_same_as=res))
        # pt(res, Unbalanced(scaled_by_same_as=df), match=df)
        
        pt(df, res, Replicated())
    end

    @partitioned df res begin res = DataFrames.deepcopy(df) end

    DataFrame(res, copy(df.nrows))
end

function Base.copy(df::DataFrame)::DataFrame
    res = Future()

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pt(df, Distributed(df, scaled_by_same_as=res))
        pt(res, ScaledBySame(as=df), match=df)

        # pt(df, Distributed(df, balanced=true))
        # pt(res, Balanced(), match=df)

        # pt(df, Distributed(df, balanced=false, scaled_by_same_as=res))
        # pt(res, Unbalanced(scaled_by_same_as=df), match=df)
        
        pt(df, res, Replicated())
    end

    @partitioned df res begin res = DataFrames.copy(df) end

    DataFrame(res, copy(df.nrows))

    # res = DataFrame(Future(), res_nrows)

    # partitioned_using() do
    #     keep_all_sample_keys(res, df)
    #     keep_sample_rate(res, df)
    # end

    # partitioned_with() do
    #     pt(df, Distributed(df))
    #     pt(res, ScaledBySame(as=df), match=df)
    # end

    # partition(df, Replicated())
    # partition(df, Blocked(dim=1, balanced=true))
    # partition(df, Blocked(dim=1, balanced=false))
    # union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    # setsample(res, :keystatistics, sample(df, :keystatistics))
    # partition_later() do
    #     union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
    #     setsample(df, :keystatistics, sample(res, :keystatistics))
    #     for key in sample(df, :allowedgroupingkeys)
    #         for balanced in [true, false]
    #             partition(df, Grouped(;key=key, balanced=balanced))
    #         end
    #     end
    # end

    # partition(res, Partitioned(), match=res)
    # mutated(res)

    # @partitioned df res begin
    #     res = copy(df)
    # end

    # DataFrame(res, copy(df.nrows))

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
    # partition(res, Partitioned(); match=df, mutating=true)
    # partition(df_nrows, ReplicatedOrReducing())
    # partition(res_nrows, Partitioned(), match=df_nrows)

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
#                 partition(res, PartitionType(), match=df)
#             elseif like
#                 partition(
#                     res,
#                     Distributing(distribution=:grouped, key=key, id='*'),
#                     match=df,
#                     on=:divisions
#                 )
#             else
#                 partition(res, Distributing(distribution=:unknown, id='*'))
#             end
#         end
#     end

#     # TODO: Use partition for res_nrows

#     @partitioned df res res_nrows begin
#         res = getindex(df)
#         res_nrows = nrow(res)
#     end

#     res
# end

function Base.getindex(df::DataFrame, rows=:, cols=:)
    # TODO: Accept either replicated, balanced, grouped by any of columns
    # in res (which is just cols of df if column selector is :), or unknown
    # and always with different ID unless row selector is :
    # and only allow : and copying columns without getting a view
    (rows isa Colon || rows isa BanyanArrays.Vector{Bool}) ||
        throw(ArgumentError("Expected selection of all rows with : or some rows with Vector{Bool}"))
    (cols != !) || throw(ArgumentError("! is not allowed for selecting all columns; use : instead"))

    # TODO: Remove this if not necessary
    if rows isa Colon && cols isa Colon
        return copy(df)
    end

    df_nrows = df.nrows
    return_vector = cols isa Symbol || cols isa String || cols isa Integer
    select_columns = !(cols isa Colon)
    filter_rows = !(rows isa Colon)
    @show sample(df)
    columns = Symbol.(names(sample(df), cols))
    cols = Future(cols)
    # # @show sample(rows)
    rows = rows isa AbstractFuture ? rows : Future(rows)
    # @show sample(rows)

    res_size =
        if filter_rows
            Future()
        elseif return_vector
            Future(df.nrows, mutation=tuple)
        else
            Future(df.nrows)
        end
    res =
        if return_vector
            BanyanArrays.Vector{eltype(sample(df)[!, collect(cols)])}(Future(), res_size)
        else
            DataFrame(Future(), res_size)
        end

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df, drifted=filter_rows)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        if filter_rows
            for (dfpt_unbalanced, respt_unbalanced, dfpt_balanced, respt_balanced) in zip(
                # unbalanced
                Distributed(df; balanced=false, filtered_to=res),
                Distributed(res; balanced=false, filtered_from=df),
                # balanced
                Distributed(df; balanced=true, filtered_to=res),
                Distributed(res; balanced=true, filtered_from=df),
            )
                # Return Blocked if return_vector or select_columns and grouping by non-selected
                return_blocked = return_vector || (dfpt_balanced.distribution == "grouped" && !(dfpt_balanced.key in columns))

                # unbalanced -> balanced
                pt(df, dfpt_unbalanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
                pt(res, (return_blocked ? Blocked(along=1) : respt_balanced) & Balanced() & Drifted())
        
                # unbalanced -> unbalanced
                pt(df, dfpt_unbalanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
                pt(res, return_blocked ? Blocked(res, along=1, balanced=false, filtered_from=df) : respt_unbalanced & Drifted())
                pt(rows, Blocked(along=1) & ScaledBySame(as=df), match=df, on=["balanced", "id"])
        
                # balanced -> unbalanced
                pt(df, dfpt_balanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
                pt(res, return_blocked ? Blocked(res, along=1, balanced=false, filtered_from=df) : respt_unbalanced & Drifted())
                pt(rows, Blocked(along=1) & ScaledBySame(as=df), match=df, on=(dfpt_balanced.distribution == "blocked" ? "balanced" : ["balanced", "id"]))
            end

            # pts_for_filtering(df, res, Blocked)
            # pt(rows, Block(along=1), match=df)

            # # blocked and balanced
            # pt(df, Blocked(along=1) & Balanced())
            # pt(rows, Blocked(along=1) & Balanced())
            # pt(res, Blocked(along=1) & Unbalanced() & Drifted())

            # pt(df, Blocked(df, balanced=false, filtered_to=res))
            # pt(rows, filter_rows ? Blocked() & Unbalanced(scaled_by_same_as=df) : Replicated())

            # for gpt in Grouped(df, filtered_to = filter_rows ? res : nothing)
            #     pt(df, gpt)
            #     # TODO: Handle select_columns
            # end

            pt(res_size, Reducing(return_vector ? quote (a, b) -> Tuple([a[1] + b[1], a[2:end]...]) end : quote (a, b) -> a .+ b end))
        else
            for dpt in Distributed(df, scaled_by_same_as=res)
                pt(df, dpt)
                if return_vector || (dpt.distribution == "grouped" && !(dpt.key in columns))
                    pt(res, Blocked(along=1) & ScaledBySame(as=df), match=df, on=["balanced", "id"])
                else
                    pt(res, ScaledBySame(as=df), match=df)
                end
            end
            pt(res_size, PartitionType(), match=df_nrows)
        end

        # if filter_rows
        #     pt(df, Blocked(df, balanced=true))
        #     pt(rows, Blocked(rows, balanced=true))

        #     pt(df, Blocked(df, balanced=false) | Grouped(df), match=rows, on=["balanced", "id"])
        #     pt(rows, Blocked(along=1))
        # else
        # end
        # pt(rows,  ? Blocked(along=1): Replicated())
        # pt(res)
        # pt(res_size)
        pt(df, res, res_size, rows, cols, Replicated())
        pt(df_nrows, Replicating())
    end

    @partitioned df df_nrows res res_size rows cols begin
        print("In getindex")
        res = df[rows, cols]
        # @show df
        # @show rows
        # @show res
        res_size = rows isa Colon ? df_nrows : size(res)
        res_size = res isa Base.Vector ? res_size : first(res_size)
    end

    # @show sample(res)

    res

    # by = names(sample(df), cols)
    # onecol = cols isa Symbol || cols isa String || cols isa Integer

    # # TODO: Maybe make @partitioned be called first so that we can have error handling
    # # get triggered first. But make sure that some stuff that needs to happen before
    # # @partitioned happens like creating futures and using mutated(future, new_future)

    # # TODO: Handle case where cols is a single column by returning a Vector

    # # TODO: Compute estimate of memory usage in code regions unless memory is
    # # already specified to be something non-zero

    # df_nrows = df.nrows
    # res_nrows = rows isa Colon ? Future(df_nrows, onecol ? tuple : identity) : Future()
    # res = onceol ? Vector(Future(), res_nrows) : DataFrame(Future(), res_nrows)
    # rows = rows isa Vector ? rows : Future(rows)
    # cols = Future(cols)

    # partition(df, Replicated())
    # partition(rows, Replicated())
    # partition(res, Replicated())
    
    # for balanced in [true, false]
    #     partition(df, Blocked(;dim=1, balanced=balanced))

    #     if rows isa Colon
    #         partition(rows, Replicated())
    #     elseif balanced
    #         partition(rows, Blocked(;dim=1, balanced=true))
    #     else
    #         partition(rows, Blocked(;dim=1); match=df, on="id")
    #     end

    #     if rows isa Colon
    #         partition(res, Blocked(;dim=1, balanced=balanced), match=df, on=["id"])
    #     else
    #         partition(res, Blocked(;dim=1, balanced=false, id="*"))
    #     end
    # end

    # # Merge sample properties of df to res
    # # TODO: Make sample properies here work with onecol to produce array
    # # TODO: Implement saving/loading functions for dataframe
    # # TODO: Implement array operations, saving/loading for them
    # if !onecol
    #     union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    #     intersect!(sample(res, :allowedgroupingkeys), by)
    #     if rows isa Colon
    #         # Only merge column statistics if the column exists after filtering
    #         for key in by
    #             if key in sample(res, :names)
    #                 setsample(res, :keystatistics, key, sample(df, :keystatistics, key))
    #             end
    #         end
    #     end
    # end

    # partition_later() do
    #     if !onecol
    #         # Merge sample properties of res to df
    #         union!(sample(df, :allowedgroupingkeys), sample(res, :allowedgroupingkeys))
    #     end

    #     # Distributed includes all Blocked and all Grouped on applicable keys
    #     # and both balanced and unbalanced
    #     # TODO: Have annotation code access and set job and sample properties
    #     # while PT constructor code is responsible for automatically
    #     # constructing constraints
    #     # TODO: Implement sample properties
    #     # - copying over from one future to another
    #     # - computing them
    #     # - renaming
    #     # - mutating
    #     for key in sample(df, :allowedgroupingkeys)
    #         for balanced in [true, false]
    #             # PT constructors serve to add in constraints needed to ensure
    #             # the parameters hold true. In other words, they make PTs
    #             # correct by construction
    #             partition(df, Grouped(;key=key, balanced=balanced))

    #             if rows isa Colon
    #                 partition(rows, Replicated())
    #             else
    #                 partition(rows, Blocked(;dim=1); match=df, on="id")
    #             end

    #             if !onecol && key in sample(res, :names)
    #                 if rows isa Colon
    #                     partition(res, Distributed(), match=df, on=["key", "divisions", "balanced", "id"])
    #                 else
    #                     partition(res, Distributed(balanced=false, id="*"), match=df, on=["key", "divisions"])
    #                 end
    #             else
    #                 if rows isa Colon
    #                     partition(res, Blocked(;dim=1), match=df, on=["balanced", "id"])
    #                 else
    #                     # PT constructors should:
    #                     # - Create MaxNPartitions and MemoryUsage constraints if balanced
    #                     # - Create random ID if *
    #                     partition(res, Blocked(;dim=1, balanced=false, id="*"))
    #                 end
    #             end
    #         end
    #     end
    # end

    # partition(cols, Replicated())
    # partition(df_nrows, ReplicatedOrReducing())
    # if rows isa Colon
    #     partition(res_nrows, Reducing(;reducer=(onceol ? .+ : +)))
    # else
    #     partition(res_nrows, ReplicatedOrReducing(match=df))
    # end

    # mutated(res)

    # @partitioned df df_nrows res res_nrows rows cols begin
    #     res = df[rows, cols]
    #     res_nrows = rows isa Colon ? df_nrows : length(res)
    #     res_nrows = res isa Vector ? (rows) : rows
    # end

    # res

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

    # res_data = cols isa Colon ? Future(;same_keys_as=df, scaled_by_same_as=df) : Future()
    # res_data = Future()

    # TODO: Maybe make some way to mutate the value according to the later
    # approximation of cols
    # TODO: Store length instead of size for DataFrame
    # res_nrows = rows isa Colon ? Future(df.size, ???) : Future()

    # res_nrows = Future()
    # res = DataFrame(res_data, res_nrows)

    # partition(df, Replicating())
    # partition(res, Replicating(), match=df)

    # partition(df, Balanced(dim=1))
    # partition(df, Distributing(distribution=:unknown))
    # if rows isa Colon
    #     partition(res, PartitionType(); match=df, on="id")
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
    #             partition(res, PartitionType(), match=df)
    #         elseif like
    #             partition(
    #                 res,
    #                 Distributing(distribution=:grouped, key=key, id='*'),
    #                 match=df,
    #                 on=:divisions
    #             )
    #         else
    #             partition(res, Distributing(distribution=:unknown, id='*'))
    #         end
    #     end
    # end

    # partition(res_nrows, rows isa Colon ? Replicated() : Reduce(dim=1))

    # TODO: Make mut accept multiple arguments
    # TODO: Implement partition

    # mutated(res, res_nrows)
    # @partitioned df res res_nrows rows cols begin
    #     res = getindex(df, rows)
    #     res_nrows = size(res)
    # end
end

function Base.setindex!(df::DataFrame, v::Union{BanyanArrays.Vector, BanyanArrays.Matrix, DataFrame}, rows, cols)
    rows isa Colon || throw(ArgumentError("Cannot mutate a subset of rows in place"))

    # selection = names(sample(df), cols)

    res = Future()
    # cols = Future(Symbol.(names(sample(df), cols)))
    cols = Future(cols)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        for dpt in Distributed(df, scaled_by_same_as=res)
            pt(df, dpt)
            pt(res, ScaledBySame(as=df), match=df)

            # The array that we are inserting into this dataframe must be
            # partitioned with the same ID or it must be perfectly balanced
            # if the original dataframe is also balanced.
            if dpt.distribution == "blocked" && dpt.balanced
                pt(v, Blocked(along=1) & Balanced())
            else
                pt(v, Blocked(along=1), match=df, on=["balanced", "id"])
            end
        end

        pt(df, res, v, cols, Replicated())
    end

    # # union!(sample(res, :allowedgroupingkeys), sample(df, :allowedgroupingkeys))
    # # # Only merge column statistics if the column is not mutated by setindex!
    # # for key in sample(df, :names)
    # #     if !(key in selection)
    # #         setsample(res, :keystatistics, key, sample(df, :keystatistics, key))
    # #     end
    # # end

    # partition(df, Replicated())
    # partition(col, Replicated())
    
    # partition(df, Blocked(dim=1, balanced=true))
    # partition(v, Blocked(dim=1, balanced=true))
    
    # partition(df, Blocked(dim=1, balanced=false))
    # partition(res, Partitioned(); match=df)

    # # TODO: Implement partition_later to end the current PA and store
    # # function that can potentially (but may also not) create a new PA

    # partition_later() do
    #     for key in sample(res, :allowedgroupingkeys)
    #         if key in sample(df, :names)
    #             push!(sample(res, :allowedgroupingkeys))
    #         end
    #     end
    #     for key in sample(res, :names)
    #         if !(key in selection)
    #             setsample(df, :keystatistics, key, sample(res, :keystatistics, key))
    #         end
    #     end

    #     for key in sample(df, :allowedgroupingkeys)
    #         for balanced in [true, false]
    #             partition(df, Grouped(;key=key, balanced=balanced))
    #             if key in selection
    #                 partition(res, Blocked(dim=1, balanced=balanced); match=df, on="id")
    #             else
    #                 partition(res, Partitioned(); match=df)
    #             end
    #         end
    #     end 
    # end

    # partition(v, Blocked(dim=1, balanced=false), match=df, on="id")
    # partition(cols, Replicated())

    # # res_data = cols isa Colon ? Future()
    # # res_nrows = rows isa Colon ? Future(df.size) : Future()
    # # res = DataFrame(res_data, res_nrows)

    # # mut will replace df with the future and sample and location of res and
    # # record a DestroyRequest on the original future in df. The contents of
    # # res are modified to ensure that when its destructor is called a
    # # DestroyRequest is not called on the new future which is now stored in df.
    # # This could be done by setting the value_id to nothing and having the
    # # finalizer of Future only record a DestroyRequest if the value ID isn't
    # # set to nothing. This should also indicate that a sample should not be
    # # collected for the thing being mutated.
    # # TODO: Implement this version of mut
    # mutated(df, res)

    @partitioned df v cols res begin
        df[:, cols] = v
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

function DataFrames.rename(df::DataFrame, args...; kwargs...)
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_all_sample_keys_renamed(res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        # distributed
        for dfpt in Distributed(df, scaled_by_same_as=res)
            pt(dfpt)
            if dfpt.distribution == "grouped"
                groupingkeyindex = indexin(dfpt.key, sample(df, :keys))
                groupingkey = sample(res, :keys)[groupingkeyindex]
                pt(res, Grouped(by=groupingkey) & ScaledBySame(as=df), match=df, on=["balanced", "id", "divisions", "rev"])
            else
                pt(res, ScaledBySame(as=df), match=df)
            end
        end
        
        # replicated
        pt(df, res, args, kwargs, Replicated())
    end

    @partitioned df res args kwargs begin
        res = DataFrames.rename(df, args...; kwargs...)
    end

    DataFrame(res, copy(df.nrows))


    # TODO: Make partition_delayed calls be processed in reverse
    # TODO: Populate groupingkeys of samples forwards and backwards
    # TODO: Copy over sample properties forwards and backwards

    # res = Future()
    # args = Future(args)
    # kwargs = Future(kwargs)

    # partition(df, Replicated())
    # partition(res, Replicated())
    
    # for balanced in [true, false]
    #     partition(df, Blocked(;dim=1, balanced=balanced))
    #     partition(res, Blocked(;dim=1, balanced=balanced), match=df, on=["id"])
    # end

    # partition_later() do
    #     allowedgroupingkeys = sample(res, :allowedgroupingkeys)
    #     statistics = sample(res, :keystatistics, key)
    #     for (i, key) in enumerate(sample(res, :names))
    #         prevkey = sample(df, :names)[i]
    #         if key in keys(statistics)
    #             setsample(df, :keystatistics, prevkey, statistics)
    #         end
    #         if key in allowedgroupingkeys
    #             push!(sample(df, :allowedgroupingkeys), prevkey)
    #             for balanced in [true, false]
    #                 partition(df, Grouped(;key=prevkey, balanced=balanced))
    #                 partition(res, Grouped(;key=key), match=df, on=["divisions", "balanced", "id"])
    #             end
    #         end
    #     end
    # end

    # # for k in keys(d)
    # #     gpt, max_ngroups = GroupedBy(df; key=k)
    # #     partition(df, gpt, at_most=max_ngroups)
    # #     partition(df, Grouped(res; key=k), match=df, on=[:divisions, :id])
    # # end
    # # partition() do job
    # #     for (gpt, max_ngroups) in GroupedBy(df, stalekeys)
    # #         partition(df, gpt, at_most=max_ngroups)
    # #         partition(res)
    # #     end
    # # end

    # # res = DataFrame()
    # # partition(df, GroupedBy())
    # # partition(res, GroupedBy(), match=df, on=["distribution", "axis", "divisions"])
    # # res

    # mutated(res)

    # @partitioned df res args kwargs begin
    #     res = rename(df, args...; kwargs...)
    # end

    # allowedgroupingkeys = sample(df, :allowedgroupingkeys)
    # statistics = sample(df, :keystatistics, key)
    # for (i, key) in enumerate(sample(df, :names))
    #     newkey = sample(res, :names)[i]
    #     if key in allowedgroupingkeys
    #         push!(sample(res, :allowedgroupingkeys), newkey)
    #     end
    #     if key in keys(statistics)
    #         setsample(res, :keystatistics, newkey, statistics[key])
    #     end
    # end

    # DataFrame(res, copy(df.nrows))
end

# Make AtMost only accept a value (we can support PT references in the future if needed)
            # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
            # that the constraint can be satisfied for this PT to be used# TODO: Implement select/transform/combine for DataFrame
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

function Base.sort(df::DataFrame, cols=:; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of sorted dataframe"))

    # TODO: Support a UserColOrdering passed into cols
    # # Determine what to sort by and whether to sort in reverse
    # firstcol = first(cols)
    # sortingkey, isreversed = if firstcol isa DataFrames.UserColOrdering
    #     if isempty(firstcols.kwargs)
    #         firstcols.col, get(kwargs, :rev, false)
    #     elseif length(firstcol.kwargs) == 1 && haskey(firstcol.kwargs, :rev)
    #         firstcols.col, get(firstcol.kwargs, :rev, false)
    #     else
    #         throw(ArgumentError("Only rev is supported for ordering"))
    #     end
    # else
    #     first(names(sample(df), firstcol)), get(kwargs, :rev, false)
    # end

    res = Future()
    columns = Symbol.(names(sample(df), cols))
    cols = Future(cols)
    kwargs = Future(kwargs)
    sortingkey = first(columns)
    isreversed = get(collect(kwargs), :rev, false)

    # TODO: Change to_vector(x) to [x;]

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_sample_keys(sortingkey, res, df)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        # We must construct seperate PTs for balanced=true and balanced=false
        # because these different PTs have different required constraints
        # TODO: Implement reversed in Grouped constructor
        pt(df, Grouped(df, by=sortingkey, rev=isreversed) | Replicated())
        # The Match constraint only applies to PT parameters. So we also need
        # a constraint to ensure that however `df` is grouped (balanced or
        # unbalanced), `res` is scaled by the same factor to account for
        # unbalanced distribution.
        pt(res, ScaledBySame(as=df), match=df)
        pt(df, res, cols, kwargs, Replicated())
    end

    # mutated(res)

    @partitioned df res cols kwargs begin
        res = DataFrames.sort(df, cols; kwargs...)
    end

    # statistics = sample(df, :keystatistics, by)
    # if by in keys(statistics)
    #     setsample(res, :keystatistics, by, statistics[by])
    # end

    DataFrame(res, copy(df.nrows))
end

function DataFrames.innerjoin(dfs::DataFrame...; on, kwargs...)
    length(dfs) >= 2 || throw(ArgumentError("Join requires at least 2 dataframes"))

    # TODO: Make it so that the code region's sampled computation is run first to allow for the function's
    # error handling to kick in first

    # TODO: Change this annotation to allow for grouping on any of the keys we
    # are joining on

    groupingkeys = first(on isa Base.Vector ? on : [on])
    groupingkeys = Symbol.(groupingkeys isa Union{Tuple,Pair} ? [groupingkeys...] : Base.fill(groupingkeys, length(dfs)))

    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    on = Future(on)
    kwargs = Future(kwargs)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        # NOTE: `to_vector` is necessary here (`[on...]` won't cut it) because
        # we allow for a vector of pairs
        # TODO: Determine how to deal with the fact that some groupingkeys can
        # stick around in the case that we are doing a broadcast join
        # TODO: Make it so that the function used here and in groupby/sort
        # simply adds in the grouping key that was used
        keep_sample_keys_named(
            [df => groupingkey for (df, groupingkey) in zip(dfs, groupingkeys)]...,
            res => first(groupingkeys),
            drifted = true,
        )
        # NOTE: We are adjusting the sample rate accordingly, but we still need
        # to note that skew can occur in the selectivity of the join.
        # Therefore, we create ScaleBy constraints just for the
        # selectivity/skew issue - not for the sample rate.
        keep_sample_rate(res, dfs...)
    end

    # TODO: Use something like this for join
    partitioned_with() do
        # unbalanced, ...., unbalanced -> balanced - "partial sort-merge join"
        dfs_with_groupingkeys = [df => groupingkey for (df, groupingkey) in zip(dfs, groupingkeys)]
        for (df, groupingkey) in dfs_with_groupingkeys
            pt(df, Grouped(df, by=groupingkey, balanced=false, filtered_to=(res=>first(groupingkeys))), match=res, on=["divisions", "rev"])
        end
        pt(res, Grouped(res, by=first(groupingkeys), balanced=true, filtered_from=dfs_with_groupingkeys) & Drifted())

        # balanced, unbalanced, ..., unbalanced -> unbalanced
        for i in 1:length(dfs)
            # "partial sort-merge join"
            for (j, (df, groupingkey)) in enumerate(dfs_with_groupingkeys)
                pt(df, Grouped(df, by=groupingkey, balanced=(j==i), filtered_to=(res => first(groupingkeys))), match=dfs[i], on=["divisions", "rev"])
            end
            pt(res, Grouped(res, by=first(groupingkeys), balanced=false, filtered_from=dfs_with_groupingkeys[i]) & Drifted(), match=dfs[i], on=["divisions", "rev"])

            # broadcast join
            pt(dfs[i], Distributed(dfs[i]))
            for (j, df) in enumerate(dfs)
                if j != i
                    pt(df, Replicated())
                end
            end
            pt(res, ScaledBySame(as=dfs[i]), match=dfs[i])

            # TODO: Ensure that constraints are copied backwards properly everywhere
        end

        # TODO: Implement a nested loop join using Cross constraint. To
        # implement this, we may need a new PT constructor thaor some new
        # way of propagating ScaleBy constraints
        # for dpts in IterTools.product([Distributed(dpt, )]...)
        # pt(dfs..., Distributing(), cross=dfs)
        # pg(res, Blocked() & Unbalanced() & Drifted())

        # unbalanced, unbalanced, ... -> unbalanced - "partial sort-merge join"
        for (df, groupingkey) in dfs_with_groupingkeys
            pt(df, Grouped(df, by=groupingkey, balanced=false, filtered_to=(res => first(groupingkeys))), match=res, on=["divisions", "rev"])
        end
        pt(res, Grouped(res, by=first(groupingkeys), balanced=false, filtered_from=dfs_with_groupingkeys) & Drifted())
        
        # "replicated join"
        pt(res_nrows, Reducing(quote (a, b) -> a .+ b end))
        pt(dfs..., on, kwargs, res, res_nrows, Replicated())

        # TODO: Support nested loop join where multiple are Block and Cross-ed and others are all Replicate
    end

    @partitioned dfs on kwargs res res_nrows begin
        res = DataFrames.innerjoin(dfs...; on=on, kwargs...)
        res_nrows = nrow(res)
    end

    # partition(dfs..., Replicated())
    # partition(df2, Replicated())
    # partition(res, Replicated())
    # partition(res_nrows, Replicated())

    # for balanced in [true, false]
    #     partition(df1, Replicated())
    #     partition(df2, Replicated())
    #     partition(res, Blocked())
    # end

    # # TODO: Support broadcast join
    # # TODO: Implement proper join support where different parties are used for
    # # determining the distribution
    # # TODO: Implement distributed from balanced + unbalanced => unbalanced, unbalanced => unbalanced
    # # TODO: Maybe make MatchOn prefer one argument over the other so that we can take both cases of
    # # different sides of the join having their divisions used

    # on = [k isa Pair ? k : (k => k) for k in (on isa Vector ? on : [on])]
    # on_left = first.(on)
    # union!(sample(res, :allowedgroupingkeys), on_left)
    # union!(sample(df1, :allowedgroupingkeys), on_left)
    # union!(sample(df2, :allowedgroupingkeys), last.(on))
    # # No statistics are copied over because joins are selective and previous
    # # statistics would no longer apply
    # for key in on
    #     for balanced in [true, false]
    #         partition(df1, Grouped(;key=first(key), balanced=balanced))
    #         partition(df2, Grouped(;key=last(key), balanced=balanced))
    #         partition(res, Grouped(;balanced=false, id="*"), match=df1, on=["key", "divisions"])
    #     end
    # end

    # partition(res_nrows, Reducing(;reducer=+))
    # partition(kwargs, Replicated())

    # mutated(res)
    # mutated(res_nrows)

    # @partitioned df1 df2 res res_nrows kwargs begin
    #     res = innerjoin(df1, df2; kwargs...)
    #     res_nrows = nrow(res)
    # end

    res
end

function DataFrames.unique(df::DataFrame, cols=:; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Returning a view of a Banyan data frame is not yet supported"))
    !(cols isa Pair || (cols isa Function && !(cols isa Colon))) || throw(ArgumentError("Unsupported specification of columns for which to get unique rows"))

    # TOOD: Just reuse select here

    # TODO: Check all usage of first
    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    columns = Symbol.(names(sample(df), cols))
    cols = Future(cols)
    kwargs = Future(kwargs)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_sample_keys(columns, res, df, drifted=true)
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pts_for_filtering(df, res, with=Grouped, by=columns)
        pt(res_nrows, Reducing(quote (a, b) -> a .+ b end))
        pt(df, res, res_nrows, cols, kwargs, Replicated())
    end

    @partitioned df res res_nrows cols kwargs begin
        res = DataFrames.unique(df, cols; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    res

    # res_nrows = Future()
    # res = DataFrame(Future(), res_nrows)
    # cols = Future(cols)

    # partition(df, Replicated())
    # partition(res, Replicated())
    # partition(res_nrows, Replicated())

    # # TODO: Only use at most first 8 for allowedgroupingkeys
    # by = isnothing(cols) ? sample(df, :names) : names(sample(df), cols)
    # union!(sample(df, :allowedgroupingkeys), by)
    # union!(sample(res, :allowedgroupingkeys), by)
    # for key in by
    #     for balanced in [true, false]
    #         partition(df, Grouped(;key=key, balanced=balanced))
    #     end
    # end

    # partition(res, Grouped(;balanced=false, id="*"), match=df, on=["key", "divisions"])
    # partition(res_nrows, Reducing(;reducer=+))
    # partition(cols, Replicated())

    # mutated(res)
    # mutated(res_nrows)

    # @partitioned df res res_nrows cols begin
    #     res = isnothing(cols) ? unique(df) : unique(df, cols)
    #     res_nrows = nrow(res)
    # end

    # res
end

function DataFrames.nonunique(df::DataFrame, cols=:; kwargs...)
    !get(kwargs, :view, false) || throw(ArgumentError("Cannot return view of Banyan dataframe"))
    !(cols isa Pair || (cols isa Function && !(cols isa Colon))) || throw(ArgumentError("Unsupported specification of columns for which to get unique rows"))

    # TOOD: Just reuse select here

    df_nrows = df.nrows
    res_size = Future(df.nrows, mutation=tuple)
    res = BanyanArrays.Vector{Bool}(Future(), res_size)
    columns = Symbol.(names(sample(df), cols))
    cols = Future(cols)
    kwargs = Future(kwargs)

    partitioned_using_modules("DataFrames")
    partitioned_using() do
        keep_sample_rate(res, df)
    end

    partitioned_with() do
        pt(df, Grouped(df, by=columns))
        pt(res, Blocked(along=1) & ScaledBySame(as=df), match=df, on=["balanced", "id"])
        pt(df_nrows, Replicating())
        pt(res_size, PartitionType(), match=df_nrows)
        pt(df, res, df_nrows, res_size, cols, kwargs, Replicated())
    end

    @partitioned df df_nrows res res_size cols kwargs begin
        res = DataFrames.nonunique(df, cols; kwargs...)
        # @show "nonunique" res
        res_size = Tuple(df_nrows)
    end

    res

    # if get(kwargs, :view, false) throw(ArgumentError("Cannot return view of Banyan dataframe")) end

    # res_nrows = Future()
    # res = DataFrame(Future(), res_nrows)
    # cols = Future(cols)

    # partition(df, Replicated())
    # partition(res, Replicated())
    # partition(res_nrows, Replicated())

    # by = isnothing(cols) ? sample(df, :names) : names(sample(df), cols)
    # union!(sample(df, :allowedgroupingkeys), by)
    # union!(sample(res, :allowedgroupingkeys), by)
    # for key in by
    #     for balanced in [true, false]
    #         partition(df, Grouped(;key=key, balanced=balanced))
    #     end
    # end

    # partition(res, Grouped(;balanced=false, id="*"), match=df, on=["key", "divisions"])
    # partition(res_nrows, Reducing(;reducer=+))
    # partition(cols, Replicated())

    # # TODO: Maybe make mut automatic or re-evaluate exactly what it is used for
    # mutated(res)
    # mutated(res_nrows)

    # @partitioned df res res_nrows cols begin
    #     res = isnothing(cols) ? nonunique(df) : nonunique(df, cols)
    #     res_nrows = nrow(res)
    # end

    # res
end

# TODO: Implement SubDataFrame
