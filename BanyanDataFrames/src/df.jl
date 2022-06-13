mutable struct DataFrame <: AbstractFuture
    data::Future
    nrows::Future
    # TODO: Add offset for indexing
    # offset::Future
end

Base.convert(::Type{DataFrame}, df::DataFrames.DataFrame) = DataFrame(Future(df; datatype="DataFrame"), Future(nrow(df)))

Banyan.convert(::Type{Future}, df::DataFrame) = df.data
Banyan.sample(df::DataFrame)::DataFrames.DataFrame = sample(df.data)

const DFSampleForGrouping = SampleForGrouping{DataFrames.DataFrame,String}

function _sample_df_for_grouping(df::Future, cols::Future)::DFSampleForGrouping
    df_sample::DataFrames.DataFrame = sample(df)
    groupingkeys::Base.Vector{String} = names(df_sample, sample(cols))
    df_sample_for_grouping::DFSampleForGrouping = DFSampleForGrouping(df, df_sample, groupingkeys, Int64[1])
    df_sample_for_grouping
end

function _get_groupingkeys(on)::Base.Vector{String}
    groupingkeys = on isa Base.AbstractVector ? on[1] : [on]
    groupingkeys = if groupingkeys isa Pair
        Base.collect(groupingkeys)
    else
        Base.fill(groupingkeys, length(dfs))
    end
    map(string, groupingkeys)
end

# DataFrame sample

Banyan.sample_axes(df::DataFrames.DataFrame) = Int64[1]
Banyan.sample_keys(df::DataFrames.DataFrame)::Base.Vector{String} = names(df)
Banyan.sample_by_key(df::DataFrames.DataFrame, key::String) = df[!, key]


# DataFrame properties

DataFrames.nrow(df::DataFrame) = compute(df.nrows)
DataFrames.ncol(df::DataFrame) = ncol(sample(df)::DataFrames.DataFrame)
Base.size(df::DataFrame) = (nrow(df), ncol(df))
Base.ndims(df::DataFrame) = 2
Base.names(df::DataFrame, args...) = names(sample(df), args...)
Base.propertynames(df::DataFrame) = propertynames(sample(df)::DataFrames.DataFrame)

# DataFrame creation

function read_table(path::String; kwargs...)
    @nospecialize
    df_loc = RemoteTableSource(path; kwargs...)
    df_loc.src_name == "Remote" || error("$path does not exist")
    df_loc_nrows::Int64 = df_loc.src_parameters["nrows"]
    df_nrows = Future(df_loc_nrows)
    DataFrame(Future(datatype="DataFrame", source=df_loc), df_nrows)
end

# TODO: For writing functions, if a file is specified, enforce Replicated

Partitioned(f::Future) = begin
    res = Distributed(sample_for_grouping(f, String))
    push!(res, Replicated())
    res
end
pts_for_partitioned(futures::Base.Vector{Future}) = pt(futures[1], Partitioned(futures[1]))

function write_table(df::DataFrame, path)
    partitioned_computation(
        pts_for_partitioned,
        df,
        destination=RemoteTableDestination(path),
        new_source=_->RemoteTableSource(path)
    )
end

function Banyan.compute_inplace(df::DataFrame)
    partitioned_computation(pts_for_partitioned, df, destination=Disk())
end

# DataFrame filtering

function _pts_for_filtering(init::Future, final::Future, initpts_unbalanced::Base.Vector{PartitionType}, finalpts_unbalanced::Base.Vector{PartitionType}, initpts_balanced::Base.Vector{PartitionType}, finalpts_balanced::Base.Vector{PartitionType})
    for i in 1:length(initpts_unbalanced)
        initpt_unbalanced::PartitionType = initpts_unbalanced[i]
        finalpt_unbalanced::PartitionType = finalpts_unbalanced[i]
        initpt_balanced::PartitionType = initpts_balanced[i]
        finalpt_balanced::PartitionType = finalpts_balanced[i]

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

function pts_for_filtering(init::Future, final::Future)
    # There should be a balanced and unbalanced PT for each possible key
    # that the initial/final data can be grouped on

    init_sample::DFSampleForGrouping = sample_for_grouping(init, String)
    final_sample::DFSampleForGrouping = sample_for_grouping(final, String)

    # unbalanced
    initpts_unbalanced = Distributed(init_sample; balanced=false, filtered_relative_to=final_sample, filtered_from=false)
    finalpts_unbalanced = Distributed(final_sample; balanced=false, filtered_relative_to=init_sample, filtered_from=true)
    # balanced
    initpts_balanced = Distributed(init_sample; balanced=true, filtered_relative_to=final_sample, filtered_from=false)
    finalpts_balanced = Distributed(final_sample; balanced=true, filtered_relative_to=init_sample, filtered_from=true)

    _pts_for_filtering(init, final, initpts_unbalanced, finalpts_unbalanced, initpts_balanced, finalpts_balanced)
end

function pts_for_filtering(init::Future, final::Future, groupingkeys::Base.Vector{String})
    # There should be a balanced and unbalanced PT for each possible key
    # that the initial/final data can be grouped on

    init_sample::DFSampleForGrouping = sample_for_grouping(init, groupingkeys)
    final_sample::DFSampleForGrouping = sample_for_grouping(final, groupingkeys)

    # unbalanced
    initpts_unbalanced = Grouped(init_sample; balanced=false, filtered_relative_to=final_sample, filtered_from=false)
    finalpts_unbalanced = Grouped(final_sample; balanced=false, filtered_relative_to=init_sample, filtered_from=true)

    # balanced
    initpts_balanced = Grouped(init_sample; balanced=true, filtered_relative_to=final_sample, filtered_from=false)
    finalpts_balanced = Grouped(final_sample; balanced=true, filtered_relative_to=init_sample, filtered_from=true)

    _pts_for_filtering(init, final, initpts_unbalanced, finalpts_unbalanced, initpts_balanced, finalpts_balanced)
end

pts_for_filtering(init::AbstractFuture, final::AbstractFuture) =
    pts_for_filtering(convert(Future, init), convert(Future, final))
pts_for_filtering(init::AbstractFuture, final::AbstractFuture, groupingkeys::Base.Vector{String}) =
    pts_for_filtering(convert(Future, init), convert(Future, final), groupingkeys)

function pts_for_dropmissing(futures::Base.Vector{Future})
    df, res, res_nrows, args, kwargs = futures
    pts_for_filtering(df, res)
    pt(res_nrows, Reducing(+))
    pt(df, res, res_nrows, args, kwargs, Replicated())
end

function _dropmissing(df::Future, res_nrows::Future, res::Future, args::Future, kwargs::Future)
    partitioned_with(pts_for_dropmissing, [df, res, res_nrows, args, kwargs], scaled=[df, res], keep_same_keys=true, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res res_nrows args kwargs begin
        res = DataFrames.dropmissing(df, args...; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.dropmissing(df::DataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    !get(kwargs, :view, false)::Bool || throw(ArgumentError("Cannot return view of filtered dataframe"))

    res_nrows = Future()
    res = Future(datatype="DataFrame")
    args = Future(args)
    kwargs = Future(kwargs)

    # We need to maintain these sample properties and hold constraints on
    # memory usage so that we can properly handle data skew

    _dropmissing(df.data, res_nrows, res, args, kwargs)
end

function pts_for_filter(futures)
    df, f, res_nrows, res, kwargs = futures
    pts_for_filtering(df, res)
    pt(res_nrows, Reducing(+))
    pt(df, res, res_nrows, f, kwargs, Replicated())
end

function _filter(df::Future, f::Future, res_nrows::Future, res::Future, kwargs::Future)
    partitioned_with(pts_for_filter, Future[df, f, res_nrows, res, kwargs], scaled=[df, res], keep_same_keys=true, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res res_nrows f kwargs begin
        res = DataFrames.filter(f, df; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    DataFrame(res, res_nrows)
end

function Base.filter(f, df::DataFrame; kwargs...)
    @nospecialize
    !get(kwargs, :view, false)::Bool || throw(ArgumentError("Cannot return view of filtered dataframe"))

    f = Future(f)
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    kwargs = Future(kwargs)

    _filter(df.data, f, res_nrows, res, kwargs)
end

# TODO: Make a `used` field and ensure that splitting/merging functions don't get used if their used are not provided

# DataFrame element-wise

function pts_for_copy_df(futures::Base.Vector{Future})
    df, res = futures
    df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, String)
    pt(df, Distributed(df_sample_for_grouping, scaled_by_same_as=res))
    pt(res, ScaledBySame(df), match=df)
    pt(df, res, Replicated())
end

function Missings.allowmissing(df::DataFrame)::DataFrame
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")

    partitioned_with(pts_for_copy_df, [df.data, res], scaled=[df.data, res], keep_same_keys=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res begin res = DataFrames.allowmissing(df) end

    DataFrame(res, res_nrows)
end

function Missings.disallowmissing(df::DataFrame)::DataFrame
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")

    partitioned_with(pts_for_copy_df, [df.data, res], scaled=[df.data, res], keep_same_keys=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res begin res = DataFrames.disallowmissing(df) end

    DataFrame(res, res_nrows)
end

function Base.deepcopy(df::DataFrame)::DataFrame
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")

    partitioned_with(pts_for_copy_df, [df.data, res], scaled=[df.data, res], keep_same_keys=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res begin res = DataFrames.deepcopy(df) end

    DataFrame(res, res_nrows)
end

function Base.copy(df::DataFrame)::DataFrame
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")

    partitioned_with(pts_for_copy_df, [df.data, res], scaled=[df.data, res], keep_same_keys=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res begin res = DataFrames.copy(df) end

    DataFrame(res, res_nrows)
end

# DataFrame column manipulation (with filtering)

add_sizes(a::NTuple{1,Int64}, b::NTuple{1,Int64}) = NTuple{1,Int64}(a[1] + b[1])

function pts_for_getindex(futures::Base.Vector{Future})
    df::Future, df_nrows::Future, cols::Future, rows::Future, res::Future, res_size::Future = futures
    df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, String)
    cols_sample = sample(cols)
    rows_sample = sample(rows)
    columns::Base.Vector{String} = names(df_sample_for_grouping.sample, cols_sample)
    return_vector = cols_sample isa Symbol || cols_sample isa String || cols_sample isa Integer
    filter_rows = !(rows_sample isa Colon)
    if filter_rows
        res_sample_for_grouping = sample_for_grouping(res, return_vector ? Int64 : String)
        for (dfpt_unbalanced, respt_unbalanced, dfpt_balanced, respt_balanced) in zip(
            # unbalanced
            Distributed(df_sample_for_grouping; balanced=false, filtered_relative_to=res_sample_for_grouping, filtered_from=false),
            Distributed(res_sample_for_grouping; balanced=false, filtered_relative_to=df_sample_for_grouping, filtered_from=true),
            # balanced
            Distributed(df_sample_for_grouping; balanced=true, filtered_relative_to=res_sample_for_grouping, filtered_from=false),
            Distributed(res_sample_for_grouping; balanced=true, filtered_relative_to=df_sample_for_grouping, filtered_from=true),
        )
        # TODO: Ensure Grouped can take dataframe and array
            # Return Blocked if return_vector or select_columns and grouping by non-selected
            return_blocked = return_vector || (dfpt_balanced.parameters["distribution"]::String == "grouped" && !(dfpt_balanced.parameters["key"] in columns))

            # unbalanced -> balanced
            pt(df, dfpt_unbalanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
            pt(res, (return_blocked ? BlockedAlong(1) : respt_balanced) & Balanced() & Drifted())
    
            # unbalanced -> unbalanced
            pt(df, dfpt_unbalanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
            pt(res, return_blocked ? Blocked(res, along=[1], balanced=false, filtered_from=df) : respt_unbalanced & Drifted())
            pt(rows, BlockedAlong(1) & ScaledBySame(df), match=df, on=["balanced", "id"])
    
            # balanced -> unbalanced
            pt(df, dfpt_balanced, match=(return_blocked ? nothing : res), on=["distribution", "key", "divisions", "rev"])
            pt(res, return_blocked ? Blocked(res, along=[1], balanced=false, filtered_from=df) : respt_unbalanced & Drifted())
            pt(rows, BlockedAlong(1) & ScaledBySame(df), match=df, on=(dfpt_balanced.parameters["distribution"]::String == "blocked" ? "balanced" : ["balanced", "id"]))
        end

        pt(res_size, Reducing(return_vector ? add_sizes : +))
    else
        for dpt in Distributed(df_sample_for_grouping, scaled_by_same_as=res)
            pt(df, dpt)
            if return_vector || (dpt.parameters["distribution"]::String == "grouped" && !(dpt.parameters["key"]::String in columns))
                pt(res, BlockedAlong(1) & ScaledBySame(df), match=df, on=["balanced", "id"])
            else
                pt(res, ScaledBySame(df), match=df)
            end
        end
        pt(res_size, PartitionType(), match=df_nrows)
    end
    
    pt(df, res, res_size, rows, cols, Replicated())
    pt(df_nrows, Replicating())
end

function _getindex(df_sample::DataFrames.DataFrame, df::Future, df_nrows::Future, return_vector::Bool, filter_rows::Bool, cols::Future, rows::Future, res::Future, res_size::Future)
    partitioned_with(pts_for_getindex, [df, df_nrows, cols, rows, res, res_size], scaled=[df, res], keep_same_keys=!return_vector, drifted=filter_rows, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df df_nrows res res_size rows cols begin
        res = df[rows, cols]
        return_vector = res isa Base.AbstractVector
        res_length = if rows isa Colon
            df_nrows
        elseif return_vector
            length(res)
        else
            nrow(res)
        end
        res_length::Int64
        res_size = return_vector ? tuple(res_length) : res_length
    end

    if return_vector
        T = eltype(df_sample[!, sample(cols)])
        res = BanyanArrays.Vector{T}(res, res_size)
    else
        res = DataFrame(res, res_size)
    end
end

function Base.getindex(df::DataFrame, rows=:, cols=:)
    @nospecialize
    # We accept either replicated, balanced, grouped by any of columns
    # in res (which is just cols of df if column selector is :), or unknown
    # and always with different ID unless row selector is :
    # and only allow : and copying columns without getting a view
    (rows isa Colon || rows isa BanyanArrays.Vector{Bool}) ||
        throw(ArgumentError("Expected selection of all rows with : or some rows with Vector{Bool}"))
    (cols != !) || throw(ArgumentError("! is not allowed for selecting all columns; use : instead"))

    if rows isa Colon && cols isa Colon
        return copy(df)
    end

    df_nrows = df.nrows
    return_vector = cols isa Symbol || cols isa String || cols isa Integer
    filter_rows = !(rows isa Colon)
    cols = Future(cols)
    rows::Future = rows isa AbstractFuture ? convert(Future, rows) : Future(rows)

    res_size::Future =
        if filter_rows
            Future()
        elseif return_vector
            Future(from=df.nrows, mutation=tuple)
        else
            Future(from=df.nrows)
        end
    res = Future(datatype = return_vector ? "Array" : "DataFrame")

    _getindex(sample(df.data), df.data, df_nrows, return_vector, filter_rows, cols, rows, res, res_size)
end

function pts_for_setindex(futures::Base.Vector{Future})
    df, v, res, cols = futures
    df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, String)
    for dpt in Distributed(df_sample_for_grouping, scaled_by_same_as=res)
        pt(df, dpt)
        pt(res, ScaledBySame(df), match=df)

        # The array that we are inserting into this dataframe must be
        # partitioned with the same ID or it must be perfectly balanced
        # if the original dataframe is also balanced.
        if dpt.parameters["distribution"]::String == "blocked" && dpt.balanced
            pt(v, BlockedAlong(1) & Balanced())
        else
            pt(v, BlockedAlong(1), match=df, on=["balanced", "id"])
        end
    end

    pt(df, res, v, cols, Replicated())
end

function _setindex(df::Future, v::Future, res::Future, cols::Future)
    partitioned_with(pts_for_setindex, [df, v, res, cols], scaled=[df, res], keep_same_keys=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df v cols res begin
        df[:, cols] = v
        res = df
    end
end

function Base.setindex!(df::DataFrame, v::Union{BanyanArrays.Vector, BanyanArrays.Matrix, DataFrame}, rows, cols)
    @nospecialize
    rows isa Colon || throw(ArgumentError("Cannot mutate a subset of rows in place"))

    res = Future(datatype="DataFrame", mutate_from=df.data)
    cols = Future(cols)

    _setindex(convert(Future, df), convert(Future, v), res, cols)
end

function pts_for_rename(futures::Base.Vector{Future})
    df::Future, res::Future, args::Future, kwargs::Future = futures
    res_sample::DataFrames.DataFrame = sample(res)
    df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, String)
    for dfpt in Distributed(df_sample_for_grouping, scaled_by_same_as=res)
        pt(dfpt)
        if dfpt.parameters["distribution"]::String == "grouped"
            dfpt_key::String = dfpt.parameters["key"]
            groupingkeyindex = indexin(dfpt_key, sample_keys(df_sample_for_grouping.sample)::Base.Vector{String})
            groupingkey = sample_keys(res_sample)[groupingkeyindex]
            pt(res, GroupedBy(groupingkey) & ScaledBySame(df), match=df, on=["balanced", "id", "divisions", "rev"])
        else
            pt(res, ScaledBySame(df), match=df)
        end
    end
    
    # replicated
    pt(df, res, args, kwargs, Replicated())
end

function _rename(df::Future, res_nrows::Future, res::Future, args::Future, kwargs::Future)::DataFrame
    partitioned_with(pts_for_rename, [df, res, args, kwargs], scaled=[df, res], keep_same_keys=true, renamed=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res args kwargs begin
        res = DataFrames.rename(df, args...; kwargs...)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.rename(df::DataFrame, args...; kwargs...)::DataFrame
    @nospecialize
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")
    args = Future(args)
    kwargs = Future(kwargs)
    df_sample::DataFrames.DataFrame = sample(df)

    _rename(df.data, res_nrows, res, args, kwargs)
end

# DataFrame shuffling

function pts_for_sort(futures::Base.Vector{Future})
    df, res, cols, kwargs = futures

    # We must construct seperate PTs for balanced=true and balanced=false
    # because these different PTs have different required constraints
    # TODO: Implement reversed in Grouped constructor
    isreversed = get(kwargs, :rev, false)::Bool
    df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, String)
    pt(df, Grouped(df_sample_for_grouping, rev=isreversed) | Replicated())
    # The Match constraint only applies to PT parameters. So we also need
    # a constraint to ensure that however `df` is grouped (balanced or
    # unbalanced), `res` is scaled by the same factor to account for
    # unbalanced distribution.
    pt(res, ScaledBySame(df), match=df)
    pt(df, res, cols, kwargs, Replicated())
end

function _sort(df::Future, res_nrows::Future, res::Future, cols::Future, kwargs::Future, sortingkey::String)::DataFrame
    partitioned_with(pts_for_sort, [df, res, cols, kwargs], scaled=[df, res], keys=[sortingkey], modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res cols kwargs begin
        res = DataFrames.sort(df, cols; kwargs...)
    end

    DataFrame(res, res_nrows)
end

function Base.sort(df::DataFrame, cols=:; kwargs...)::DataFrame
    @nospecialize
    !get(kwargs, :view, false)::Bool || throw(ArgumentError("Cannot return view of sorted dataframe"))

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

    df_sample::DataFrames.DataFrame = sample(df)
    res_nrows = copy(df.nrows)
    res = Future(datatype="DataFrame")
    columns::Base.Vector{String} = names(df_sample, cols)
    cols = Future(cols)
    kwargs = Future(kwargs)
    sortingkey = first(columns)

    _sort(df.data, res_nrows, res, cols, kwargs, sortingkey)
end

function pts_for_innerjoin(futures::Base.Vector{Future})
    dfs = futures[1:end-4]
    res_nrows, res, on, kwargs = futures[end-3:end]
    groupingkeys = _get_groupingkeys(sample(on))
    # unbalanced, ...., unbalanced -> balanced - "partial sort-merge join"
    dfs_with_groupingkeys = Dict{Future,String}(df => groupingkey for (df, groupingkey) in zip(dfs, groupingkeys))
    dfs_with_groupingkeys::Base.Vector{DFSampleForGrouping} = DFSampleForGrouping[]
    for (df, groupingkey) in zip(dfs, groupingkeys)
        df_sample_for_grouping::DFSampleForGrouping = sample_for_grouping(df, groupingkey)
        push!(dfs_with_groupingkeys, df_sample_for_grouping)
    end
    res_sample_for_groupingkeys::DFSampleForGrouping = sample_for_groupingkeys(res, first(groupingkeys))
    for df_with_groupingkey in dfs_with_groupingkeys
        pt(df, Grouped(df_with_groupingkey, balanced=false, filtered_relative_to=res_sample_for_groupingkeys, filtered_from=false), match=res, on=["divisions", "rev"])
    end
    pt(res, Grouped(res_sample_for_groupingkeys, balanced=true, filtered_relative_to=dfs_with_groupingkeys, filtered_from=true) & Drifted())

    # balanced, unbalanced, ..., unbalanced -> unbalanced
    for i in 1:length(dfs)
        # "partial sort-merge join"
        for (j, df_with_groupingkey) in enumerate(dfs_with_groupingkeys)
            pt(df, Grouped(df_with_groupingkey, balanced=(j==i), filtered_relative_to=res_sample_for_groupingkeys, filtered_from=false), match=dfs[i], on=["divisions", "rev"])
        end
        pt(res, Grouped(res_sample_for_groupingkeys, balanced=false, filtered_relative_to=first(dfs_with_groupingkeys), filtered_from=true) & Drifted(), match=dfs[i], on=["divisions", "rev"])

        # broadcast join
        # TODO: Allow this Distributed to be Grouped on keys that aren't
        # the keys used for this join. That does require taking a new
        # sample_for_grouping that doesn't restrict the keys
        pt(dfs[i], Distributed(dfs_with_groupingkeys[i]))
        for (j, df) in enumerate(dfs)
            if j != i
                pt(df, Replicated())
            end
        end
        pt(res, ScaledBySame(dfs[i]), match=dfs[i])

        # TODO: Ensure that constraints are copied backwards properly everywhere
    end

    # TODO: Implement a nested loop join using Cross constraint. To
    # implement this, we may need a new PT constructor thaor some new
    # way of propagating ScaleBy constraints
    # for dpts in IterTools.product([Distributed(dpt, )]...)
    # pt(dfs..., Distributing(), cross=dfs)
    # pg(res, Blocked() & Unbalanced() & Drifted())

    # unbalanced, unbalanced, ... -> unbalanced - "partial sort-merge join"
    for df_with_groupingkey in dfs_with_groupingkeys
        pt(df, Grouped(df_with_groupingkey, balanced=false, filtered_relative_to=res_sample_for_groupingkeys, filtered_from=false), match=res, on=["divisions", "rev"])
    end
    pt(res, Grouped(res_sample_for_groupingkeys, balanced=false, filtered_relative_to=dfs_with_groupingkeys, filtered_from=true) & Drifted())
    
    # "replicated join"
    pt(res_nrows, Reducing(+))
    pt(dfs..., on, kwargs, res, res_nrows, Replicated())

    # TODO: Support nested loop join where multiple are Block and Cross-ed and others are all Replicate
end

function _innerjoin(dfs::Base.Vector{Future}, groupingkeys::Base.Vector{String}, res_nrows::Future, res::Future, on::Future, kwargs::Future, keys_by_future::Base.Vector{Tuple{Future,Base.Vector{String}}})::DataFrame
    scaled_dfs::Base.Vector{Future} = copy(dfs)
    push!(dfs, res)
    futures::Base.Vector{Future} = copy(scaled_dfs)
    append!(futures, Future[res_nrows, on, kwargs])
    partitioned_with(
        pts_for_innerjoin,
        futures,
        scaled=scaled_dfs,
        # NOTE: We are adjusting the sample rate accordingly, but we still need
        # to note that skew can occur in the selectivity of the join.
        # Therefore, we create ScaleBy constraints just for the
        # selectivity/skew issue - not for the sample rate.
        # The sample rate multiplies since this is a join
        keep_sample_rate=false,
        # NOTE: `to_vector` is necessary here (`[on...]` won't cut it) because
        # we allow for a vector of pairs
        # TODO: Determine how to deal with the fact that some groupingkeys can
        # stick around in the case that we are doing a broadcast join
        # TODO: Make it so that the function used here and in groupby/sort
        # simply adds in the grouping key that was used
        keys_by_future=keys_by_future,
        drifted=true,
        modules=["BanyanDataFrames.DataFrames"],
        keytype=String
    )

    @partitioned dfs on kwargs res res_nrows begin
        res = DataFrames.innerjoin(dfs...; on=on, kwargs...)
        res_nrows = nrow(res)
    end

    # # TODO: Support broadcast join
    # # TODO: Implement proper join support where different parties are used for
    # # determining the distribution
    # # TODO: Implement distributed from balanced + unbalanced => unbalanced, unbalanced => unbalanced
    # # TODO: Maybe make MatchOn prefer one argument over the other so that we can take both cases of
    # # different sides of the join having their divisions used

    DataFrame(res, res_nrows)
end

function DataFrames.innerjoin(dfs::DataFrames.DataFrame...; on, kwargs...)::DataFrame
    @nospecialize
    length(dfs) >= 2 || throw(ArgumentError("Join requires at least 2 dataframes"))

    # TODO: Make it so that the code region's sampled computation is run first to allow for the function's
    # error handling to kick in firstDis

    # TODO: Change this annotation to allow for grouping on any of the keys we
    # are joining on

    groupingkeys = _get_groupingkeys(on)
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    on = Future(on)
    kwargs = Future(kwargs)

    # TODO: Use something like this for join
    keys_by_future::Base.Vector{Tuple{Future,Base.Vector{String}}} = Tuple{Future,Base.Vector{String}}[
        (df, String[groupingkey]) for (df, groupingkey) in zip(dfs, groupingkeys)
    ]
    push!(keys_by_future, (res, groupingkeys[1:1]))

    _innerjoin(convert(Base.Vector{Future}, dfs), groupingkeys, res_nrows, res, on, kwargs, keys_by_future)
end

function pts_for_unique(futures::Base.Vector{Future})
    df::Future, res_nrows::Future, res::Future, cols::Future, kwargs::Future = futures
    df_sample::DataFrames.DataFrame = sample(df)
    columns::Base.Vector{String} = names(df_sample, cols)
    pts_for_filtering(df, res, columns)
    pt(res_nrows, Reducing(+))
    pt(df, res, res_nrows, cols, kwargs, Replicated())
end

function _unique(df::Future, res_nrows::Future, res::Future, columns::Base.Vector{String}, cols::Future, kwargs::Future)::DataFrame
    partitioned_with(pts_for_unique, Future[df, res_nrows, res, cols, kwargs], scaled=[df, res], keys=columns, drifted=true, modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df res res_nrows cols kwargs begin
        res = DataFrames.unique(df, cols; kwargs...)
        res_nrows = DataFrames.nrow(res)
    end

    DataFrame(res, res_nrows)
end

function DataFrames.unique(df::DataFrame, cols=:; kwargs...)::DataFrame
    @nospecialize
    !get(kwargs, :view, false)::Bool || throw(ArgumentError("Returning a view of a Banyan data frame is not yet supported"))
    !(cols isa Pair || (cols isa Function && !(cols isa Colon))) || throw(ArgumentError("Unsupported specification of columns for which to get unique rows"))

    # TOOD: Just reuse select here

    # TODO: Check all usage of first
    res_nrows = Future()
    res = Future(datatype="DataFrame")
    df_sample::DataFrames.DataFrame = sample(df)
    columns::Base.Vector{String} = names(df_sample, cols)
    cols = Future(cols)
    kwargs = Future(kwargs)

    _unique(df.data, res_nrows, res, columns, cols, kwargs)
end

function pts_for_nonunique(futures::Base.Vector{Future})
    df, df_nrows, res_size, res, cols, kwargs = futures
    df_sample_for_grouping::DFSampleForGrouping = _sample_df_for_grouping(df, cols)
    pt(df, Grouped(df_sample_for_grouping))
    pt(res, BlockedAlong(1) & ScaledBySame(df), match=df, on=["balanced", "id"])
    pt(df_nrows, Replicating())
    pt(res_size, PartitionType(), match=df_nrows)
    pt(df, res, df_nrows, res_size, cols, kwargs, Replicated())
end

function _nonunique(df::Future, df_nrows::Future, res_size::Future, res::Future, cols::Future, kwargs::Future)::BanyanArrays.Vector{Bool}
    partitioned_with(pts_for_nonunique, [df, df_nrows, res_size, res, cols, kwargs], scaled=[df, res], modules=["BanyanDataFrames.DataFrames"], keytype=String)

    @partitioned df df_nrows res res_size cols kwargs begin
        res = DataFrames.nonunique(df, cols; kwargs...)
        res_size = Tuple(df_nrows)
    end

    BanyanArrays.Vector{Bool}(res, res_size)
end

function DataFrames.nonunique(df::DataFrame, cols=:; kwargs...)::BanyanArrays.Vector{Bool}
    @nospecialize
    !get(kwargs, :view, false)::Bool || throw(ArgumentError("Cannot return view of Banyan dataframe"))
    !(cols isa Pair || (cols isa Function && !(cols isa Colon))) || throw(ArgumentError("Unsupported specification of columns for which to get unique rows"))

    # TOOD: Just reuse select here

    df_nrows = df.nrows
    
    res_size = Future(from=df.nrows, mutation=tuple)
    res = Future(datatype="Array")
    
    cols = Future(cols)
    kwargs = Future(kwargs)

    _nonunique(df.data, df_nrows, res_size, res, columns, cols, kwargs)
end

# TODO: Implement SubDataFrame