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
    isdefined(BanyanDataFrames, Symbol("#34#37")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#34#37"))})   # time: 0.23418552
    isdefined(BanyanDataFrames, Symbol("#135#136")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#135#136"))})   # time: 0.06206502
    isdefined(BanyanDataFrames, Symbol("#116#119")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#116#119"))})   # time: 0.02680686
    Base.precompile(Tuple{typeof(groupby),DataFrame,Symbol})   # time: 0.025035787
    Base.precompile(Tuple{typeof(sample_memory_usage),DataFrames.GroupedDataFrame{DataFrames.DataFrame}})   # time: 0.005951188

    # Additional precompilation

    # pfs.jl and utils_pfs.jl
    for V in [SVector{32,UInt8}, SVector{1,Int64}]
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
        Base.precompile(Tuple{Core.kwftype(typeof(read_parquet)),NamedTuple{(:shuffled, :source_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_parquet),String})   # time: 15.114132
    end
    if isdefined(BanyanDataFrames, :ReadBlockCSV)
        push!(ReadBlockFuncs, ReadBlockCSV)
        push!(WriteFuncs, WriteCSV)
        Base.precompile(Tuple{Core.kwftype(typeof(read_csv)),NamedTuple{(:shuffled, :source_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_csv),String})   # time: 15.114132
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
    precompile(
        get_remote_table_source,
        (
            String,
            Location,
            Sample,
            Bool
        )
    )

    # df.jl
    precompile(Banyan.sample_divisions, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_max_ngroups, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_max, (DataFrames.DataFrame, String))
    precompile(Banyan.sample_min, (DataFrames.DataFrame, String))
    precompile(pts_for_filtering, (Future, Future))
    precompile(pts_for_filtering, (Future, Future, Base.Vector{String}))

    # pt_lib_constructors.jl
    precompile(
        Banyan.make_grouped_pt,
        (
            Future,
            DataFrames.DataFrame,
            String,
            Bool,
            Bool,
            Bool,
            Dict{Future,Base.Vector{String}},
            Dict{Future,Base.Vector{String}},
            Base.Vector{Future},
        )
    )
    precompile(
        Banyan.make_grouped_pts,
        (
            Future,
            DataFrames.DataFrame,
            Base.Vector{String},
            Base.Vector{Bool},
            Bool,
            Bool,
            Dict{Future,Base.Vector{String}},
            Dict{Future,Base.Vector{String}},
            Base.Vector{Future},
        )
    )
    precompile(
        Banyan.get_factor,
        (
            Float64,
            DataFrames.DataFrame,
            DataFrames.DataFrame,
            String,
            String,
        )
    )
    for T in (String, Int32, Int64, Float64, BigFloat, Dates.Date, Dates.DateTime, Bool)
        # (for getindex)
        precompile(
            Banyan.get_factor,
            (
                Float64,
                DataFrames.DataFrame,
                Base.Vector{T},
                String,
                String,
            )
        )
    end

    # df.jl and gdf.jl
    precompile(_dropmissing, (Future, Future, Future, Future, Future))
    precompile(_filter, (Future, Future, Future, Future))
    precompile(_getindex, (Future, Future, Bool, Bool, Bool, Base.Vector{String}, Future, Future, Future))
    precompile(_setindex, (Future, Future, Future, Future))
    precompile(_rename, (Future, Future, Future, Future, Future, DataFrames.DataFrame))
    precompile(_sort, (Future, Future, Future, Future, Bool, Future, String))
    precompile(_innerjoin, (Base.Vector{Future}, Base.Vector{String}, Future, Future, Future, Future, Dict{Future,Base.Vector{String}}))
    precompile(_unique, (Future, Future, Future, Base.Vector{String}, Future, Future))
    precompile(_nonunique, (Future, Future, DataFrames.DataFrame, Future, Future, Base.Vector{String}, Future, Future))
    precompile(partitioned_for_groupby, (Future, Future, Base.Vector{String}, Future, Future, Future))
    precompile(partitioned_with_for_select, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_select, (Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_transform, (Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_with_for_combine, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_combine, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
    precompile(partitioned_for_subset, (Future, Future, Future, Future, Base.Vector{String}, Future, Future, Future, Future))
end
