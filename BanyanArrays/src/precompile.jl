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

    # Additional precompilation

    NDElTypes = [Int32, Int64, Float32, Float64, Bool]
    ArrayTypes = [
        Base.Array{T,N}
        for N in 1:4
        for T in NDElTypes
    ]
    ElTypes = [String,  BigFloat, Dates.Date, Dates.DateTime]
    append!(
        ArrayTypes,
        [
            Base.Vector{T}
            for T in ElTypes
        ]
    )
    append!(ElTypes, NDElTypes)

    OHTypes = [Base.Vector{UInt8}, Base.Vector{Int64}, Base.Vector{Float64}, Base.Vector{Dates.DateTime}]

    # pfs.jl and utils_pfs.jl
    precompile(
        ReadBlockHelperJuliaArray,
        (
            Nothing,
            Dict{String,Any},
            Int64,
            Int64,
            MPI.Comm,
            String,
            Dict{String,Any},
            String,
            Int64
        )
    )
    precompile(CopyFromJuliaArray, (Nothing, Dict{String,Any}, Int64, Int64, MPI.Comm, String, Dict{String,Any}))
    for ArrayType in [ArrayTypes; Empty]
        precompile(
            WriteJuliaArrayHelper,
            (
                Nothing,
                ArrayType,
                Dict{String,Any},
                Int64,
                Int64,
                MPI.Comm,
                String,
                Dict{String,Any},
                String,
                Int64
            )
        )
        precompile(
            RebalanceArray,
            (
                ArrayType,
                Dict{String,Any},
                Dict{String,Any},
                MPI.Comm,
                Int64
            )
        )
        precompile(
            ConsolidateArray,
            (
                ArrayType,
                Dict{String,Any},
                Dict{String,Any},
                MPI.Comm,
                Int64
            )
        )
        for V in OHTypes
            precompile(
                ShuffleArrayHelper,
                (
                    ArrayType,
                    Dict{String,Any},
                    Dict{String,Any},
                    MPI.Comm,
                    Bool,
                    Bool,
                    Bool,
                    Int64,
                    Int64,
                    Bool,
                    Base.Vector{Base.Vector{Division{V}}},
                    Base.Vector{Division{V}}
                )
            )
        end
    end
    for ArrayType in ArrayTypes
        precompile(
            CopyToJuliaArray,
            (
                Nothing,
                ArrayType,
                Dict{String,Any},
                Int64,
                Int64,
                MPI.Comm,
                String,
                Dict{String,Any},
            )
        )
        for V in OHTypes
            precompile(
                SplitGroupArray,
                (
                    ArrayType,
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
                    Int64,
                    Bool,
                    IdDict{Any,Any},
                )
            )
        end
    end
    precompile(read_julia_array_file, (String, UnitRange{Int64}, UnitRange{Int64}, Int64))
    for ArrayType in ArrayTypes
        precompile(
            Banyan.split_on_executor,
            (
                ArrayType,
                Int64,
                UnitRange{Int64}
            )
        )
    end
    # merge_on_executor

    # # locations.jl
    # precompile(
    #     get_remote,
    #     (
    #         String,
    #         Location,
    #         Sample,
    #         Bool
    #     )
    # )

    # df.jl
    for AT in ArrayTypes
        precompile(Banyan.orderinghashes, (AT, Int64))
        precompile(Banyan.sample_by_key, (AT, Int64))
        precompile(Banyan.sample_divisions, (AT, Int64))
        precompile(Banyan.sample_max_ngroups, (AT, Int64))
        precompile(Banyan.sample_max, (AT, Int64))
        precompile(Banyan.sample_min, (AT, Int64))
    end
    precompile(add_sizes_on_axis, (Int64,))

    # pt_lib_constructors.jl
    for ArrayType in ArrayTypes
        for (T, K, TF, KF) in [
            (ArrayType, Int64, Nothing, Int64)
            # For now, we don't really have to worry about filtering operations
            # on grouped arrays. When we do, we might want to precompile to
            # reduce overhead.
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
        precompile(
            Banyan.make_grouped_balanced_pt,
            (
                SampleForGrouping{ArrayType,Int64},
                Int64,
                Bool,
                Bool,
            )
        )
    end

    # df.jl and gdf.jl
    for pts_for_func in [pts_for_blocked_and_replicated, pts_for_collect, pts_for_copying, pts_for_fill, pts_for_getindex, pts_for_map, pts_for_map_params, pts_for_mapslices, pts_for_reduce, pts_for_replicating, pts_for_sortslices]
        precompile(pts_for_func, (Base.Vector{Future},))
    end
    precompile(_getindex, (Future, Future, Future, Future))
    precompile(_mapslices, (Future, Future, Future, Future, Future))
    precompile(_reduce, (Future, Future, Future, Future, Future, Future))
    precompile(_sortslices, (Future, Int64, Future, Future, Future))
    precompile(partitioned_for_collect, (Future, Future))
    precompile(partitioned_for_fill, (Future, Future, Future))
end
