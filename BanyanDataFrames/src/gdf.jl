mutable struct GroupedDataFrame <: AbstractFuture
    data::Future
    length::Future
    parent::DataFrame
    groupcols::Future
    groupkwargs::Future
end

Banyan.convert(::Type{Future}, gdf::GroupedDataFrame) = gdf.data
Banyan.isview(gdf::GroupedDataFrame) = true
Banyan.sample_memory_usage(gdf::DataFrames.GroupedDataFrame)::Int64 =
    sample_memory_usage(gdf) - sample_memory_usage(parent(gdf))

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
    # We can use distributed because downstream functions might constrain it to
    # just grouped partitioning if needed.
    pt(df, Distributed(df_sample_for_grouping, scaled_by_same_as=gdf))
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

    partitioned_for_groupby(df.data, gdf, gdf_length, cols, kwargs)

    res = GroupedDataFrame(gdf, gdf_length, DataFrame(df, df_nrows), cols, kwargs)
    
    res
end

# GroupedDataFrame column manipulation

function pts_for_select(futures::Base.Vector{Future})
    gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs = futures
    groupingkeys::Base.Vector{String} = _get_res_groupingkeys(true, gdf_parent, sample(groupcols))
    gdf_parent_sample_for_grouping_keys::DFSampleForGrouping = sample_for_grouping(gdf_parent, groupingkeys)
    pt(gdf_parent, Grouped(gdf_parent_sample_for_grouping_keys, scaled_by_same_as=res))
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
    res_nrows = copy(gdf.parent.nrows)
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
    res_nrows = copy(gdf.parent.nrows)
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
    rpts = ReducingGroupBy(sample(groupcols), sample(groupkwargs), sample(args), sample(kwargs))
    for rpt in rpts
        if Banyan.INVESTIGATING_REDUCING_GROUPBY
            @show rpt
        end
        pt(gdf_parent, BlockedAlong(1) & Balanced())
        pt(gdf_parent, BlockedAlong(1) & Unbalanced(res))
        pt(res, rpt & ScaledBySame(gdf_parent))
    end
    pts_for_filtering(gdf_parent, res, groupingkeys)
    # TODO: Make a ReducingGroupBy PT constructor that is similar to Reducing but takes in groupcols, groupkwargs, args, kwargs to determine the reducing_op and finishing_op
    # TODO: Iterate over result of ReducingGroupBy and, annotate gdf_parent with Blocked and res with the ReducingGroupBy
    pt(gdf, BlockedAlong(1) & ScaledBySame(gdf_parent))
    pt(res_nrows, Reducing(+)) # TODO: Change to + if possible
    # pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
    pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
end

function partitioned_for_combine(gdf_parent::Future, gdf::Future, res_nrows::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    partitioned_with(pts_for_combine, Future[gdf_parent, gdf, res_nrows, res, groupcols, groupkwargs, args, kwargs], scaled=[gdf_parent, gdf], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)
    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if isempty(gdf_parent)
            # This is needed because `combine` will fail for empty data with
            # no groups when you have reducing operations like `minimum` that
            # don't have an `init` specified.
            res = DataFrames.DataFrame()
        else
            if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
                gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
            end
            res = DataFrames.combine(gdf, args...; kwargs...)
            set_parent(res, gdf)
        end
        res_nrows = DataFrames.nrow(res)
    end
end

function DataFrames.combine(gdf::GroupedDataFrame, args...; kwargs...)::DataFrame
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

    partitioned_for_combine(gdf_parent, gdf.data, res_nrows, res, res_groupingkeys, groupcols, groupkwargs, args, kwargs)

    DataFrame(res, res_nrows)
end

function partitioned_for_subset(gdf_parent::Future, gdf::Future, res_nrows::Future, res::Future, res_groupingkeys::Base.Vector{String}, groupcols::Future, groupkwargs::Future, args::Future, kwargs::Future)
    # We don't annotate res as scaled because in an NYC trip data benchmark,
    # it was overestimating the actual
    # memory usage as 1024x the sampled memory usage when the actual was just
    # 3x the sampled.
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
