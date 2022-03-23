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
Banyan.sample_memory_usage(gdf::DataFrames.GroupedDataFrame) =
    total_memory_usage(gdf) - total_memory_usage(parent(gdf))

Base.length(gdf::GroupedDataFrame) = compute(gdf.length)
Base.size(gdf::GroupedDataFrame) = Tuple(length(gdf))
Base.ndims(gdf::GroupedDataFrame) = 1
DataFrames.groupcols(gdf::GroupedDataFrame) = groupcols(sample(gdf))
DataFrames.valuecols(gdf::GroupedDataFrame) = valuecols(sample(gdf))

# NOTE: For now we don't allow grouped dataframes to be copied since we are
# only supporting simple use-cases where you want to aggregate or transform
# or filter your grouped dataframe.

# NOTE: We don't need to implement any of the sample computation functions
# (that we implement in `df.jl`) because a `GroupedDataFrame` will never be
# assigned a `Grouped` PT. The sample computation functions are only used by
# the `Grouped` PT constructor. And we never want to assign the `Grouped` PT
# constructor to `GroupedDataFrame`s. `Blocked` will be sufficient.

# GroupedDataFrame creation

function DataFrames.groupby(df::DataFrame, cols::Any; kwargs...)::GroupedDataFrame
    # We will simply pass the `cols` and `kwargs` into `Future` constructor
    # so specialization isn't really needed
    @nospecialize
    get(kwargs, :sort, true) || error("Groups cannot currently be ordered by how they originally appeared")

    groupingkeys::Base.Vector{String} = names(sample(df), cols)

    df_nrows = df.nrows
    gdf_data = Future()
    gdf_length = Future()
    cols = Future(cols)
    kwargs = Future(kwargs)
    gdf = Future(datatype="GroupedDataFrame")

    # partition(df, Replicated())
    # partition(gdf, Replicated())
    # partition(gdf_length, Replicated())

    partitioned_with(scaled=[df, gdf], modules="DataFrames") do
        pt(df, Grouped(df, by=groupingkeys, scaled_by_same_as=gdf))
        # TODO: Avoid circular dependency
        # TODO: Specify key for Blocked
        # TODO: Ensure that bangs in splitting functions in PF library are used
        # appropriately
        pt(gdf, Blocked(along=1) & ScaledBySame(as=df))
        pt(gdf_length, Reducing(quote + end)) # TODO: See if we can `using Banyan` on the cluster and avoid this
        pt(df, gdf, gdf_length, cols, kwargs, Replicated())
    end

    @partitioned df gdf gdf_length cols kwargs begin
        gdf = DataFrames.groupby(df, cols; kwargs...)
        gdf_length = DataFrames.length(gdf)
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

    @show typeof(gdf)
    @show typeof(gdf_length)

    res = GroupedDataFrame(gdf, gdf_length, DataFrame(df, df_nrows), cols, kwargs)
    @show res
    @show typeof(res)
    res
end

# GroupedDataFrame column manipulation

function DataFrames.select(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res = Future(datatype="DataFrame")
    res_nrows = copy(gdf_parent.nrows)
    args = Future(args)
    groupingkeys::Base.Vector{String} = names(sample(gdf_parent), compute(groupcols)::Base.Vector{String})
    res_groupingkeys::Base.Vector{String} = get(kwargs, :keepkeys, true) ? groupingkeys : String[]
    kwargs = Future(kwargs)

    partitioned_with(scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules="DataFrames") do
        pt(gdf_parent, Grouped(gdf_parent, by=groupingkeys, scaled_by_same_as=res), match=res)
        pt(gdf, Blocked(along=1) & ScaledBySame(as=res))
        pt(res, ScaledBySame(as=gdf_parent))
        pt(gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    # partition(gdf, Replicated())
    # partition(gdf_parent, Replicated())
    # partition(res, Replicated())

    # # TODO: Share sampled names if performance is impacted by repeatedly getting names

    # # allowedgroupingkeys = names(sample(gdf_parent), compute(groupcols))
    # # allowedgroupingkeys = get(Base.collect(groupkwargs), :sort, false) ? allowedgroupingkeys[1:1] : allowedgroupingkeys
    # # union!(sample(gdf_parent, :allowedgroupingkeys), allowedgroupingkeys)
    # if get(Base.collect(kwargs), :keepkeys, true)
    #     union!(sample(res, :allowedgroupingkeys), sample(gdf, :allowedgroupingkeys))
    # end
    # for key in sample(gdf_parent, :allowedgroupingkeys)
    #     setsample(res, :keystatistics, key, sample(gdf_parent, :keystatistics, key))
    #     for balanced in [true, false]
    #         partition(gdf_parent, Grouped(;key=key, balanced=balanced))
    #         if get(Base.collect(kwargs), :keepkeys, true)
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
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.select(gdf, args...; kwargs...)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.transform(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res = Future(datatype="DataFrame")
    res_nrows = copy(gdf_parent.nrows)
    args = Future(args)
    groupingkeys::Base.Vector{String} = names(sample(gdf_parent), compute(groupcols))
    res_groupingkeys::Base.Vector{String} = get(kwargs, :keepkeys, true)::Bool ? groupingkeys : String[]
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame

    # TODO: Maybe automatically infer sample properties (set with
    # `partitioned_using`) by looking at the actual annotations in
    # `partitioned_with`

    partitioned_with(scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules="DataFrames") do
        pt(gdf_parent, Grouped(gdf_parent, by=groupingkeys, scaled_by_same_as=res), match=res)
        pt(gdf, Blocked(along=1) & ScaledBySame(as=res))
        pt(res, ScaledBySame(as=gdf_parent))
        pt(gdf_parent, gdf, res, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.transform(gdf, args...; kwargs...)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.combine(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    args = Future(args)
    groupingkeys::Base.Vector{String} = names(sample(gdf_parent), compute(groupcols))
    res_groupingkeys::Base.Vector{String} = get(kwargs, :keepkeys, true) ? groupingkeys : String[]
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame

    partitioned_with(scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules="DataFrames") do
        # TODO: If we want to support `keepkeys=false`, we need to make the
        # result be Blocked and `filtered_from` the input
        pts_for_filtering(gdf_parent, res, with=Grouped, by=groupingkeys)
        pt(gdf, Blocked(along=1) & ScaledBySame(as=gdf_parent))
        pt(res_nrows, Reducing(quote + end)) # TODO: Change to + if possible
        # pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
        pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.combine(gdf, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.subset(gdf::GroupedDataFrame, args...; kwargs...)
    get(kwargs, :ungroup, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must produce dataframes"))
    get(kwargs, :copycols, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes cannot return a view"))
    get(kwargs, :keepkeys, true) || throw(ArgumentError("Select/transform/combine/subset operations on grouped dataframes must keep the grouping columns"))

    gdf_parent = gdf.parent
    groupcols = gdf.groupcols
    groupkwargs = gdf.groupkwargs
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    args = Future(args)
    groupingkeys::Base.Vector{String} = names(sample(gdf_parent), compute(groupcols))
    res_groupingkeys::Base.Vector{String} = get(kwargs, :keepkeys, true) ? groupingkeys : String[]
    kwargs = Future(kwargs)

    # TODO: Put groupingkeys in GroupedDataFrame

    partitioned_with(scaled=[gdf_parent, gdf, res], grouped=[gdf_parent, res], keys=res_groupingkeys, drifted=true, modules="DataFrames") do
        pts_for_filtering(gdf_parent, res, with=Grouped, by=groupingkeys)
        pt(gdf, Blocked(along=1) & ScaledBySame(as=gdf_parent))
        pt(res_nrows, Reducing(quote (a, b) -> a .+ b end))
        pt(gdf_parent, res, gdf, res_nrows, groupcols, groupkwargs, args, kwargs, Replicated())
    end

    @partitioned gdf gdf_parent groupcols groupkwargs args kwargs res res_nrows begin
        if !(gdf isa DataFrames.GroupedDataFrame) || gdf.parent !== gdf_parent
            gdf = DataFrames.groupby(gdf_parent, groupcols; groupkwargs...)
        end
        res = DataFrames.subset(gdf, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

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
