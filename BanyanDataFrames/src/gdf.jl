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

DataFrames.GroupedDataFrame <: AbstractSample

convert(::Type{Future}, gdf::GroupedDataFrame) = gdf.data

length(gdf::GroupedDataFrame) = compute(gdf.length)
groupcols(gdf::GroupedDataFrame) = groupcols(sample(gdf))
valuecols(gdf::GroupedDataFrame) = valuecols(sample(gdf))

# NOTE: For now we don't allow grouped dataframes to be copied since we are
# only supporting simple use-cases where you want to aggregate or transform
# or filter your grouped dataframe.

# GroupedDataFrame creation

function groupby(df::DataFrame, cols; kwargs...)::GroupedDataFrame
    gdf_data = Future()
    gdf_length = Future()
    cols = Future(cols)
    kwargs = Future(kwargs)
    gdf = GroupedDataFrame(Future(), gdf_length, df, cols, kwargs)

    # partition(df, Replicated())
    # partition(gdf, Replicated())
    # partition(gdf_length, Replicated())

    groupingkeys = names(sample(df), compute(cols))

    partitioned_using() do
        keep_sample_rate(gdf, df)
    end

    partitioned_with() do
        pt(df, Grouped(df, by=groupingkeys, scaled_by_same_as=gdf))
        pt(gdf, Blocked() & ScaledBySame(as=df))
        pt(gdf_length, Reducing((a, b) -> a .+ b))
        pt(df, gdf, gdf_length, cols, kwargs, Replicated())
    end

    @partitioned df gdf gdf_length cols kwargs begin
        gdf = groupby(df, cols; kwargs...)
        gdf_length = length(gdf)
    end

    # allowedgroupingkeys = names(sample(df), compute(cols))
    # allowedgroupingkeys = get(kwargs, :sort, false) ? allowedgroupingkeys[1:1] : allowedgroupingkeys
    # union!(sample(df, :allowedgroupingkeys), allowedgroupingkeys)
    # setsample(gdf, :allowedgroupingkeys, allowedgroupingkeys)
    # for key in allowedgroupingkeys
    #     for balanced in [true, false]
    #         partition(df, Grouped(;key=key, balanced=balanced))
    #     end
    #     # Grouped computes keystatistics for key for df
    #     setsample(gdf, :keystatistics, key, sample(df, :keystatistics, key))
    # end

    # pt(gdf, Blocked(;dim=1), match=df, on=["balanced", "id"])
    # ptartition(gdf_length, Reducing(;reducer=+))
    # papt(df, gdf, gdf_length, cols, kwargs, Replicated())
    # # TODO: Ensure splitting/merging functions work for Blocked on GroupedDataFrame

    # mutated(gdf)
    # mutated(gdf_length)

    # @partitioned df gdf gdf_length cols kwargs begin
    #     gdf = groupby(df, cols; kwargs...)
    #     gdf_length = length(gdf)
    # end
    
    # gdf

    # # TODO: approximate -> sample and evaluate -> compute

    # # w.r.t. keys and axes, there are several things you need to know:
    # # - reuse of columns 
    # # Create Future for result

    # # gdf = GroupedDataFrame()
    # # gdf_len = gdf.whole_len
    # # df_len = df.whole_len
    # # for (gpt, max_ngroups) in Grouped(gdf, )
    # # partition(gdf, Distributed(), parent=df)
    # # @partitioned df gdf begin end

    # # when merging a GroupedDataFrame which must be pseudogrouped,
    # # vcat the parents and the groupindices and modify the cat-ed parents
    # # to have a column for the parent index and the gorup iindex within that parent
    # # and then do a group-by on this
    # # for writing to disk, just be sure to put everything into a dataframe such that it
    # # can be read back and have a column that specifies how to group by

    gdf
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

    groupingkeys = names(sample(gdf_parent), compute(groupcols))

    partitioned_using() do
        keep_sample_keys(if get(kwargs, :keepkeys, true) groupingkeys else [] end, res, gdf_parent, drifted=true)
        keep_sample_rate(res, gdf_parent)
    end

    partitioned_with() do
        pt(gdf_parent, Grouped(df, by=groupingkeys, scaled_by_same_as=res), match=res)
        pt(gdf, Blocked() & ScaledBySame(as=res))
        pt(res, Any(scaled_by_same_as=gdf_parent))
        pt(gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    # partition(gdf, Replicated())
    # partition(gdf_parent, Replicated())
    # partition(res, Replicated())

    # # TODO: Share sampled names if performance is impacted by repeatedly getting names

    # # allowedgroupingkeys = names(sample(gdf_parent), compute(groupcols))
    # # allowedgroupingkeys = get(compute(groupkwargs), :sort, false) ? allowedgroupingkeys[1:1] : allowedgroupingkeys
    # # union!(sample(gdf_parent, :allowedgroupingkeys), allowedgroupingkeys)
    # if get(compute(kwargs), :keepkeys, true)
    #     union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    # end
    # for key in sample(gdf_parent, :allowedgroupingkeys)
    #     setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
    #     for balanced in [true, false]
    #         partition(gdf_parent, Grouped(;key=key, balanced=balanced))
    #         if get(compute(kwargs), :keepkeys, true)
    #             partition(res, Partitioned(), match=gdf_parent)
    #         else
    #             partition(res, Blocked(dim=1), match=gdf_parent, on=["balanced", "id"])
    #         end
    #     end
    # end
    # partition(gdf, Blocked(;dim=1), match=gdf_parent, on=["balanced", "id"])

    # partition(groupcols, Replicated())
    # partition(groupkwargs, Replicated())
    # partition(args, Replicated())
    # partition(kwargs, Replicated())

    # # if kwargs[:ungroup]

    # # else
    # #     res = GroupedDataFrame(gdf)
    # #     res_nrows = res.nrows
    # #     partition(gdf, Pseudogrouped())
    # #     partition(args, Replicated())
    # #     partition(kwargs, Replicated())
    # #     @partitioned gdf res res_nrows args kwargs begin
    # #         res = select(gdf, args..., kwargs...)
    # #         res_nrows = length(gdf_nrows)
    # #     end
    # # end

    # mutated(res)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if isnothing(gdf) || gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = select(gdf, args...; kwargs...)
    end

    DataFrame(res, copy(gdf_parent.nrows))
end

function transform(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res = Future()
    args = Future(args)
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame
    groupingkeys = names(sample(gdf_parent), compute(groupcols))

    partitioned_using() do
        keep_sample_keys(
            get(kwargs, :keepkeys, true) ? groupingkeys : [], res, gdf_parent,
            drifted=true
        )
        keep_sample_rate(res, gdf_parent)
    end

    # TODO: Maybe automatically infer sample properties (set with
    # `partitioned_using`) by looking at the actual annotations in
    # `partitioned_with`

    partitioned_with() do
        pt(gdf_parent, Grouped(df, by=groupingkeys, scaled_by_same_as=res), match=res)
        pt(gdf, Blocked() & ScaledBySame(as=res))
        pt(res, Any(scaled_by_same_as=gdf_parent))
        pt(gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if isnothing(gdf) || gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = transform(gdf, args...; kwargs...)
    end

    DataFrame(res, copy(gdf_parent.nrows))
end

function combine(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    args = Future(args)
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame
    groupingkeys = names(sample(gdf_parent), compute(groupcols))

    partitioned_using() do
        keep_sample_keys(
            get(kwargs, :keepkeys, true) ? groupingkeys : [], res, gdf_parent,
            drifted=true
        )
        keep_sample_rate(res, gdf_parent)
    end

    partitioned_with() do
        pts_for_filtering(gdf_parent, res, with=Grouped, by=groupingkeys)
        pt(gdf, Blocked() & ScaledBySame(as=gdf_parent))
        pt(res_nrows, Reducing((a, b) -> a .+ b))
        pt(gdf_parent, res, gdf, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if isnothing(gdf) || gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = combine(gdf, args...; kwargs...)
        res_nrows = nrows(res)
    end

    res
end

function subset(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.kwargs
    res_nrows = Future()
    res = DataFrame(Future(), res_nrows)
    args = Future(args)
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame
    groupingkeys = names(sample(gdf_parent), compute(groupcols))

    partitioned_using() do
        keep_sample_keys(
            get(kwargs, :keepkeys, true) ? groupingkeys : [], res, gdf_parent,
            drifted=true
        )
        keep_sample_rate(res, gdf_parent)
    end

    partitioned_with() do
        pts_for_filtering(gdf_parent, res, with=Grouped, by=groupingkeys)
        pt(gdf, Blocked() & ScaledBySame(as=gdf_parent))
        pt(res_nrows, Reducing((a, b) -> a .+ b))
        pt(gdf_parent, res, gdf, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if isnothing(gdf) || gdf.parent != gdf_parent
            gdf = groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = subset(gdf, args...; kwargs...)
        res_nrows = nrows(res)
    end

    res
end

# function transform(gdf::GroupedDataFrame)
#     get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
#     get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

#     gdf_parent = gdf.parent
#     groupcols = gdf.groupcols
#     groupkwargs = gdf.kwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(compute(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(compute(kwargs), :keepkeys, true)
#                 partition(res, Partitioned(), match=gdf_parent)
#             else
#                 partition(res, Blocked(dim=1), match=gdf_parent, on=["balanced", "id"])
#             end
#         end
#     end
#     partition(gdf, Blocked(;dim=1), match=gdf_parent, on=["balanced", "id"])

#     partition(groupcols, Replicated())
#     partition(groupkwargs, Replicated())
#     partition(args, Replicated())
#     partition(kwargs, Replicated())

#     mutated(res)

#     @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
#         if gdf.parent != gdf_parent
#             gdf = groupby(gdf_parent, groupcols; groupkwargs...)
#         end
#         res = transform(gdf, args...; kwargs...)
#     end

#     res
# end

# function combine(gdf::GroupedDataFrame)
#     get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
#     get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

#     gdf_parent = gdf.parent
#     groupcols = gdf.groupcols
#     groupkwargs = gdf.kwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(compute(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(compute(kwargs), :keepkeys, true)
#                 partition(res, Grouped(key=key, balanced=false, id="*"), match=gdf_parent, on="divisions")
#             else
#                 partition(res, Blocked(dim=1, balanced=false, id="*"))
#             end
#         end
#     end
#     partition(gdf, Blocked(;dim=1), match=gdf_parent, on=["balanced", "id"])

#     partition(groupcols, Replicated())
#     partition(groupkwargs, Replicated())
#     partition(args, Replicated())
#     partition(kwargs, Replicated())

#     # TODO: Allow for putting multiple variables that share a PT in a call to partition

#     mutated(res)

#     @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
#         if gdf.parent != gdf_parent
#             gdf = groupby(gdf_parent, groupcols; groupkwargs...)
#         end
#         res = combine(gdf, args...; kwargs...)
#     end

#     res
# end

# # TODO: Implement filter using some framework for having references by keeping
# # track of the lineage of which code regions produced which 

# function subset(gdf::GroupedDataFrame)
#     get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
#     get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

#     gdf_parent = gdf.parent
#     groupcols = gdf.groupcols
#     groupkwargs = gdf.kwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(compute(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(compute(kwargs), :keepkeys, true)
#                 partition(res, Grouped(key=key, balanced=false, id="*"), match=gdf_parent, on="divisions")
#             else
#                 partition(res, Blocked(dim=1, balanced=false, id="*"))
#             end
#         end
#     end
#     partition(gdf, Blocked(;dim=1), match=gdf_parent, on=["balanced", "id"])

#     partition(groupcols, Replicated())
#     partition(groupkwargs, Replicated())
#     partition(args, Replicated())
#     partition(kwargs, Replicated())

#     mutated(res)

#     @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
#         if gdf.parent != gdf_parent
#             gdf = groupby(gdf_parent, groupcols; groupkwargs...)
#         end
#         res = subset(gdf, args...; kwargs...)
#     end

#     res
# end
