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

Banyan.convert(::Type{Future}, gdf::GroupedDataFrame) = gdf.data
Banyan.isview(gdf::GroupedDataFrame) = true
Banyan.sample_memory_usage(gdf::DataFrames.GroupedDataFrame)::Int64 =
    total_memory_usage(gdf) - total_memory_usage(parent(gdf))

Base.length(gdf::GroupedDataFrame) = compute(gdf.length)
Base.size(gdf::GroupedDataFrame) = Tuple(length(gdf))
Base.ndims(gdf::GroupedDataFrame) = 1
DataFrames.groupcols(gdf::GroupedDataFrame) = groupcols(sample(gdf)::GroupedDataFrame)
DataFrames.valuecols(gdf::GroupedDataFrame) = valuecols(sample(gdf)::GroupedDataFrame)

# NOTE: For now we don't allow grouped dataframes to be copied since we are
# only supporting simple use-cases where you want to aggregate or transform
# or filter your grouped dataframe.

# NOTE: We don't need to implement any of the sample computation functions
# (that we implement in `df.jl`) because a `GroupedDataFrame` will never be
# assigned a `Grouped` PT. The sample computation functions are only used by
# the `Grouped` PT constructor. And we never want to assign the `Grouped` PT
# constructor to `GroupedDataFrame`s. `Blocked` will be sufficient.

function _get_res_groupingkeys(keepkeys::Bool, gdf_parent::Future, groupcols_sample)::Base.Vector{String}
    if keepkeys
        gdf_parent_sample::DataFrames.DataFrame = sample(gdf_parent)
        names(gdf_parent_sample, groupcols_sample)
    else
        String[]
    end
end

# GroupedDataFrame creation

function pts_for_groupby(futures::Base.Vector{Future})
    df, gdf, gdf_length, cols, kwargs = futures

    df_sample_for_grouping = _sample_df_for_grouping(df, cols)
    pt(df, Grouped(df_sample_for_grouping, scaled_by_same_as=gdf))
    # TODO: Avoid circular dependency
    # TODO: Specify key for Blocked
    # TODO: Ensure that bangs in splitting functions in PF library are used
    # appropriately
    pt(gdf, BlockedAlong(1) & ScaledBySame(df))
    pt(gdf_length, Reducing(+)) # TODO: See if we can `using Banyan` on the cluster and avoid this
    pt(df, gdf, gdf_length, cols, kwargs, Replicated())
end

function partitioned_for_groupby(df::Future, gdf::Future, gdf_length::Future, cols::Future, kwargs::Future)
    partitioned_with(pts_for_groupby, Future[df, gdf, gdf_length, cols, kwargs], scaled=Future[df, gdf], modules=String["BanyanDataFrames.DataFrames"], keytype=String)
    @partitioned df gdf gdf_length cols kwargs begin
        gdf = DataFrames.groupby(df, cols; kwargs...)
        gdf_length = DataFrames.length(gdf)
    end
end

function DataFrames.groupby(df::DataFrame, cols::Any; kwargs...)::GroupedDataFrame
    @nospecialize

    # We will simply pass the `cols` and `kwargs` into `Future` constructor
    # so specialization isn't really needed
    get(kwargs, :sort, true)::Bool || error("Groups cannot currently be ordered by how they originally appeared")

    df_nrows = df.nrows
    gdf_length = Future()
    cols = Future(cols)
    kwargs = Future(kwargs)
    gdf = Future(datatype="GroupedDataFrame")

    # partition(df, Replicated())
    # partition(gdf, Replicated())
    # partition(gdf_length, Replicated())

    partitioned_for_groupby(df.data, gdf, gdf_length, cols, kwargs)

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

    res = GroupedDataFrame(gdf, gdf_length, DataFrame(df, df_nrows), cols, kwargs)
    
    res
end

# GroupedDataFrame column manipulation

function pts_for_select(futures::Base.Vector{Future})
    gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs = futures
    groupingkeys::Base.Vector{String} = _get_res_groupingkeys(true, gdf_parent, sample(groupcols))
    gdf_parent_sample_for_grouping_keys::DFSampleForGrouping = sample_for_grouping(gdf_parent, groupingkeys)
    pt(gdf_parent, Grouped(gdf_parent_sample_for_grouping_keys, scaled_by_same_as=res), match=res)
    pt(gdf, BlockedAlong(1) & ScaledBySame(res))
    pt(res, ScaledBySame(gdf_parent))
    pt(gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs, Replicated())
end

function partitioned_for_select(gdf_parent::Future, gdf::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    partitioned_with(pts_for_select, Future[gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs], scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)
    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.select(gdf, args...; kwargs...)
    end
end

function DataFrames.select(gdf::GroupedDataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    get(kwargs, :ungroup, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent.data
    res_nrows = copy(gdf_parent.nrows)
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res = Future(datatype="DataFrame")
    args = Future(args)
    res_groupingkeys::Base.Vector{String} = _get_res_groupingkeys(get(kwargs, :keepkeys, true)::Bool, gdf_parent, sample(groupcols))
    kwargs = Future(kwargs)

    partitioned_for_select(gdf_parent, gdf.data, res, res_groupingkeys, groupcols, groupkwargs, args, kwargs)

    DataFrame(res, res_nrows)
end

function partitioned_for_transform(gdf_parent::Future, gdf::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    partitioned_with(pts_for_select, Future[gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs], scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.transform(gdf, args...; kwargs...)
    end
end

function DataFrames.transform(gdf::GroupedDataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    get(kwargs, :ungroup, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent.data
    res_nrows = copy(gdf_parent.nrows)
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res = Future(datatype="DataFrame")
    res_groupingkeys = _get_res_groupingkeys(get(kwargs, :keepkeys, true)::Bool, gdf_parent, sample(groupcols))
    args = Future(args)
    
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame

    # TODO: Maybe automatically infer sample properties (set with
    # `partitioned_using`) by looking at the actual annotations in
    # `partitioned_with`

    partitioned_for_transform(gdf_parent, gdf.data, res, res_groupingkeys, groupcols, groupkwargs, args, kwargs)

    DataFrame(res, res_nrows)
end

function pts_for_combine(futures::Base.Vector{Future})
    gdf_parent::Future, gdf::Future, res_nrows::Future, res::Future, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future = futures

    groupingkeys::Base.Vector{String} = _get_res_groupingkeys(true, gdf_parent, sample(groupcols))

    # TODO: If we want to support `keepkeys=false`, we need to make the
    # result be Blocked and `filtered_from` the input
    pts_for_filtering(gdf_parent, res, groupingkeys)
    pt(gdf, BlockedAlong(1) & ScaledBySame(gdf_parent))
    pt(res_nrows, Reducing(+)) # TODO: Change to + if possible
    # pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
    pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
end

function partitioned_for_combine(gdf_parent::Future, gdf::Future, res_nrows::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    @time begin
    partitioned_with(pts_for_combine, Future[gdf_parent, gdf, res_nrows, res, groupcols, groupkwargs, args, kwargs], scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)
    println("Time for partitioned_with_for_combine in combine:")
    end
    @time begin
    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.combine(gdf, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end
    println("Time for @partitioned in combine:")
    end 
end

function DataFrames.combine(gdf::GroupedDataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    get(kwargs, :ungroup, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    @time begin
    gdf_parent = gdf.parent.data
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    args = Future(args)
    res_groupingkeys::Base.Vector{String} = _get_res_groupingkeys(get(kwargs, :keepkeys, true)::Bool, gdf_parent, sample(groupcols))
    kwargs = Future(kwargs)
    println("Time for creating futures in combine:")
    end

    partitioned_for_combine(gdf_parent, gdf.data, res_nrows, res, res_groupingkeys, groupcols, groupkwargs, args, kwargs)

    println("Time for combine:")
    DataFrame(res, res_nrows)
end

function partitioned_for_subset(gdf_parent::Future, gdf::Future, res_nrows::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    partitioned_with(pts_for_combine, Future[gdf_parent, gdf, res_nrows, res, groupcols, groupkwargs, args, kwargs], scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)
    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.subset(gdf, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end
end

function DataFrames.subset(gdf::GroupedDataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    get(kwargs, :ungroup, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true)::Bool || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent.data
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    args = Future(args)
    res_groupingkeys::Base.Vector{String} = _get_res_groupingkeys(get(kwargs, :keepkeys, true)::Bool, gdf_parent, sample(groupcols))
    kwargs = Future(kwargs)

    partitioned_for_subset(gdf_parent, gdf.data, res_nrows, res, res_groupingkeys, groupcols, groupkwargs, args, kwargs)

    DataFrame(res, res_nrows)
end

# function transform(gdf::GroupedDataFrame)
#     get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
#     get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))

#     gdf_parent = gdf.parent
#     groupcols = gdf.groupcols
#     groupkwargs = gdf.groupkwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(Base.collect(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(Base.collect(kwargs), :keepkeys, true)
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
#         if gdf.parent !== gdf_parent
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
#     groupkwargs = gdf.groupkwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(Base.collect(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(Base.collect(kwargs), :keepkeys, true)
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
#         if gdf.parent !== gdf_parent
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
#     groupkwargs = gdf.groupkwargs
#     res = Future()
#     args = Future(args)
#     kwargs = Future(kwargs)

#     partition(gdf, Replicated())
#     partition(gdf_parent, Replicated())
#     partition(res, Replicated())
    
#     if get(Base.collect(kwargs), :keepkeys, true)
#         union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
#     end
#     for key in sample(gdf_parent, :allowedgroupingkeys)
#         for balanced in [true, false]
#             partition(gdf_parent, Grouped(;key=key, balanced=balanced))
#             if get(Base.collect(kwargs), :keepkeys, true)
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
#         if gdf.parent !== gdf_parent
#             gdf = groupby(gdf_parent, groupcols; groupkwargs...)
#         end
#         res = subset(gdf, args...; kwargs...)
#     end

#     res
# end