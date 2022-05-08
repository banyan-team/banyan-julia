const __bodyfunction__ = Dict{Method,Any}()

# Find keyword "body functions" (the function that contains the body
# as written by the developer, called after all missing keyword-arguments
# have been assigned values), in a manner that doesn't depend on
# gensymmed names.
# `mnokw` is the method that gets called when you invoke it without
# supplying any keywords.
function __lookup_kwbody__(mnokw::Method)
    function getsym(arg)
        isa(arg, Symbol) && return arg
        @assert isa(arg, GlobalRef)
        return arg.name
    end

    f = get(__bodyfunction__, mnokw, nothing)
    if f === nothing
        fmod = mnokw.module
        # The lowered code for `mnokw` should look like
        #   %1 = mkw(kwvalues..., #self#, args...)
        #        return %1
        # where `mkw` is the name of the "active" keyword body-function.
        ast = Base.uncompressed_ast(mnokw)
        if isa(ast, Core.CodeInfo) && length(ast.code) >= 2
            callexpr = ast.code[end-1]
            if isa(callexpr, Expr) && callexpr.head == :call
                fsym = callexpr.args[1]
                if isa(fsym, Symbol)
                    f = getfield(fmod, fsym)
                elseif isa(fsym, GlobalRef)
                    if fsym.mod === Core && fsym.name === :_apply
                        f = getfield(mnokw.module, getsym(callexpr.args[2]))
                    elseif fsym.mod === Core && fsym.name === :_apply_iterate
                        f = getfield(mnokw.module, getsym(callexpr.args[3]))
                    else
                        f = getfield(fsym.mod, fsym.name)
                    end
                else
                    f = missing
                end
            else
                f = missing
            end
        else
            f = missing
        end
        __bodyfunction__[mnokw] = f
    end
    return f
end

function _precompile_()
    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    # Base.precompile(Tuple{Core.kwftype(typeof(read_csv)),NamedTuple{(:shuffled, :metadata_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_csv),String})   # time: 12.008884
    # Base.precompile(Tuple{typeof(get_nrow),String,Val{:csv}})   # time: 0.16609474
    let fbody = try __lookup_kwbody__(which(filter, (Any,DataFrame,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(filter),Any,DataFrame,))
        end
    end   # time: 0.05272379
    let fbody = try __lookup_kwbody__(which(combine, (GroupedDataFrame,Any,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(combine),GroupedDataFrame,Any,))
        end
    end   # time: 0.016692607
    let fbody = try __lookup_kwbody__(which(read_table, (String,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(read_table),String,))
        end
    end   # time: 0.016141588
    Base.precompile(Tuple{typeof(sample_memory_usage),DataFrames.GroupedDataFrame{DataFrames.DataFrame}})   # time: 0.005951188

    # Additional precompilation

    # pfs.jl and utils_pfs.jl
    OHTypes = [Base.Vector{UInt8}, Base.Vector{Int64}, Base.Vector{Float64}, Base.Vector{Dates.DateTime}]
    for V in OHTypes
        precompile(
            ShuffleDataFrameHelper,
            (
                DataFrames.DataFrame,
                Dict{String,Any},
                Dict{String,Any},
                MPI.Comm,
                Bool,
                Bool,
                Bool,
                String,
                Bool,
                Base.Vector{Base.Vector{Division{V}}},
                Base.Vector{Division{V}},
                IdDict{Any,Any}
            )
        )
        precompile(
            SplitGroupDataFrame,
            (
                DataFrames.AbstractDataFrame,
                Dict{String,Any},
                Int64,
                Int64,
                MPI.Comm,
                String,
                Dict{String,Any},
                Bool,
                Base.Vector{Division{V}},
                Bool,
                Bool,
                String,
                Bool,
                IdDict{Any,Any}
            )
        )

        # df.jl
        precompile(Banyan.sample_percentile, (DataFrames.DataFrame, String, V, V))
    end
    ReadBlockFuncs = [ReadBlockArrow]
    WriteFuncs = [WriteArrow]
    if isdefined(BanyanDataFrames, :ReadBlockParquet)
        push!(ReadBlockFuncs, ReadBlockParquet)
        push!(WriteFuncs, WriteParquet)
        Base.precompile(Tuple{Core.kwftype(typeof(read_parquet)),NamedTuple{(:shuffled, :metadata_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_parquet),String})   # time: 15.114132
    end
    if isdefined(BanyanDataFrames, :ReadBlockCSV)
        push!(ReadBlockFuncs, ReadBlockCSV)
        push!(WriteFuncs, WriteCSV)
        Base.precompile(Tuple{Core.kwftype(typeof(read_csv)),NamedTuple{(:shuffled, :metadata_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_csv),String})   # time: 15.114132
    end
    for ReadBlock in ReadBlockFuncs
        precompile(
            ReadBlock,
            (
                Nothing,
                Dict{String,Any},
                Int64,
                Int64,
                MPI.Comm,
                String,
                Dict{String,Any},
            )
        )
    end
    for Write in WriteFuncs
        for P in [DataFrames.DataFrame, Empty]
            precompile(
                Write,
                (
                    Nothing,
                    P,
                    Dict{String,Any},
                    Int64,
                    Int64,
                    MPI.Comm,
                    String,
                    Dict{String,Any},
                )
            )
        end
    end
    precompile(
        RebalanceDataFrame,
        (
            DataFrames.DataFrame,
            Dict{String,Any},
            Dict{String,Any},
            MPI.Comm
        )
    )
    precompile(ConsolidateDataFrame, (DataFrames.DataFrame, Dict{String,Any}, Dict{String,Any}, MPI.Comm))
    precompile(
        Banyan.split_on_executor,
        (
            DataFrames.DataFrame,
            Int64,
            UnitRange{Int64}
        )
    )
    for DF in [
        DataFrames.DataFrame,
        DataFrames.SubDataFrame{DataFrames.DataFrame, DataFrames.Index, Base.Vector{Int64}},
        DataFrames.SubDataFrame{DataFrames.DataFrame, DataFrames.Index, UnitRange{Int64}}
    ]
        precompile(
            Banyan.merge_on_executor,
            (
                Base.Vector{DF},
                String
            )
        )
    end

    # locations.jl
    precompile(_remote_table_source, (String, Bool, Bool, Bool, Bool, Bool))

    # df.jl
    precompile(Banyan.orderinghashes, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_by_key, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_divisions, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_max_ngroups, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_max, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_min, (DataFrames.DataFrame, String))
    precompile(_pts_for_filtering, (Future, Future, Base.Vector{PartitionType}, Base.Vector{PartitionType}, Base.Vector{PartitionType}, Base.Vector{PartitionType}))
    precompile(pts_for_filtering, (Future, Future))
    precompile(pts_for_filtering, (Future, Future, Base.Vector{String}))

    # pt_lib_constructors.jl
    ElTypes = [Int32, Int64, Float64, Bool, String, BigFloat, Dates.Date, Dates.DateTime]
    VectorTypes = [Base.Vector{ElType} for ElType in ElTypes]
    for VectorType in VectorTypes
        for (T, K, TF, KF) in [
            (DataFrames.DataFrame, String, VectorType, Int64)
            (VectorType, Int64, DataFrames.DataFrame, String)
        ]
            precompile(
                Banyan.make_grouped_pts,
                (
                    SampleForGrouping{T,K},
                    Base.Vector{Bool},
                    Bool,
                    Bool,
                    Base.Vector{SampleForGrouping{TF,KF}},
                    Bool,
                    Base.Vector{Future},
                )
            )
            precompile(
                Banyan.make_grouped_filtered_pt,
                (
                    SampleForGrouping{T,K},
                    K,
                    Base.Vector{SampleForGrouping{TF,KF}},
                    Bool,
                )
            )
            precompile(
                Banyan.get_factor,
                (
                    Float64,
                    T,
                    K,
                    SampleForGrouping{TF,KF},
                    Bool
                )
            )
            precompile(
                Banyan.get_factor,
                (
                    Float64,
                    T,
                    K,
                    SampleForGrouping{TF,K},
                    Bool
                )
            )
            precompile(
                Banyan._get_factor,
                (
                    Float64,
                    Tuple{T,K},
                    Tuple{TF,KF}
                )
            )
        end
    end
    precompile(
        Banyan.make_grouped_balanced_pt,
        (
            SampleForGrouping{DataFrames.DataFrame,String},
            String,
            Bool,
            Bool,
        )
    )

    # df.jl and gdf.jl
    for pts_for_func in [pts_for_combine, pts_for_copy_df, pts_for_dropmissing, pts_for_filter, pts_for_getindex, pts_for_groupby, pts_for_innerjoin, pts_for_nonunique, pts_for_partitioned, pts_for_rename, pts_for_select, pts_for_setindex, pts_for_setindex, pts_for_sort, pts_for_unique]
        precompile(pts_for_func, (Base.Vector{Future},))
    end
    precompile(_dropmissing, (Future, Future, Future, Future, Future))
    precompile(_filter, (Future, Future, Future, Future, Future))
    precompile(_getindex, (Future, Future, Bool, Bool, Future, Future, Future, Future))
    precompile(_setindex, (Future, Future, Future, Future))
    precompile(_rename, (Future, Future, Future, Future, Future))
    precompile(_sort, (Future, Future, Future, Future, Future, String))
    precompile(_innerjoin, (Base.Vector{Future}, Base.Vector{String}, Future, Future, Future, Future, Dict{Future,Base.Vector{String}}))
    precompile(_unique, (Future, Future, Future, Base.Vector{String}, Future, Future))
    precompile(_nonunique, (Future, Future, Future, Future, Future, Future))
    precompile(partitioned_for_groupby, (Future, Future, Base.Vector{String}, Future, Future, Future))
    # precompile(partitioned_with_for_select, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future, Base.Vector{String}))
    precompile(partitioned_for_select, (Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_transform, (Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    # precompile(partitioned_with_for_combine, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future, Base.Vector{String}))
    precompile(partitioned_for_combine, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_subset, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))

    # Arrow.jl
    precompile(Arrow.write, (String,))
    precompile(Arrow.write, (DataFrames.DataFrame,))

    # df = Future()
    # gdf = Future()
    # gdf_length = Future()
    # cols = Future(:x)
    # kwargs = Future(Dict())
    
    # partitioned_with(scaled=[df, gdf]) do
    #     pt(df, Grouped(df, by=groupingkeys, scaled_by_same_as=gdf))
    #     # TODO: Avoid circular dependency
    #     # TODO: Specify key for Blocked
    #     # TODO: Ensure that bangs in splitting functions in PF library are used
    #     # appropriately
    #     pt(gdf, Blocked(1) & ScaledBySame(df))
    #     pt(gdf_length, Reducing(+)) # TODO: See if we can `using Banyan` on the cluster and avoid this
    #     pt(df, gdf, gdf_length, cols, kwargs, Replicated())
    # end

    # @partitioned df gdf gdf_length cols kwargs begin
    #     df = DataFrames.DataFrame(:x => [1,2,3,4,5])
    #     gdf = DataFrames.groupby(df, cols; kwargs...)
    #     gdf_length = DataFrames.length(gdf)
    # end

    # Banyan.finish_task()
    # empty!(Banyan.get_session().pending_requests)
end
