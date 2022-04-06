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
    Base.precompile(Tuple{Core.kwftype(typeof(read_csv)),NamedTuple{(:shuffled, :source_invalid, :sample_invalid), Tuple{Bool, Bool, Bool}},typeof(read_csv),String})   # time: 17.013178
    isdefined(BanyanDataFrames, Symbol("#29#32")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#29#32"))})   # time: 1.4618937
    Base.precompile(Tuple{typeof(groupby),DataFrame,Symbol})   # time: 1.0748885
    isdefined(BanyanDataFrames, Symbol("#132#135")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#132#135"))})   # time: 0.1290403
    isdefined(BanyanDataFrames, Symbol("#111#114")) && Base.precompile(Tuple{getfield(BanyanDataFrames, Symbol("#111#114"))})   # time: 0.08016387
    Base.precompile(Tuple{typeof(combine),GroupedDataFrame,Vararg{Any}})   # time: 0.06936745
    Base.precompile(Tuple{typeof(sample_divisions),DataFrames.DataFrame,String})   # time: 0.045820963
    let fbody = try __lookup_kwbody__(which(filter, (Function,DataFrame,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}},typeof(filter),Function,DataFrame,))
    end
end   # time: 0.015874792
    let fbody = try __lookup_kwbody__(which(groupby, (DataFrame,Any,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(groupby),DataFrame,Any,))
    end
end   # time: 0.010533528
    Base.precompile(Tuple{typeof(groupby),DataFrame,Any})   # time: 0.008026398
    Base.precompile(Tuple{typeof(sample_memory_usage),DataFrames.GroupedDataFrame{DataFrames.DataFrame}})   # time: 0.006655749
    Base.precompile(Tuple{Core.kwftype(typeof(pts_for_filtering)),NamedTuple{(:with,), Tuple{typeof(Distributed)}},typeof(pts_for_filtering),DataFrame,Future})   # time: 0.004178916
    Base.precompile(Tuple{typeof(sample_min),DataFrames.DataFrame,String})   # time: 0.002008176
end