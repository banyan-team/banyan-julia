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
    Base.precompile(Tuple{Core.kwftype(typeof(start_session)),NamedTuple{(:cluster_name, :nworkers, :sample_rate, :print_logs, :url, :branch, :directory, :dev_paths, :release_resources_after, :force_pull, :force_sync, :force_install, :store_logs_on_cluster), Tuple{String, Int64, Int64, Bool, String, String, String, Vector{String}, Int64, Bool, Bool, Bool, Bool}},typeof(start_session)})   # time: 4.367664
    isdefined(Banyan, Symbol("#141#147")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#141#147"))})   # time: 0.34917852
    let fbody = try __lookup_kwbody__(which(start_session, ())) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Union{Nothing, String},Int64,Union{Nothing, Integer},Bool,Bool,Bool,Bool,Int64,Union{Nothing, String},Vector{String},Vector{String},Bool,Union{Nothing, Vector{String}},Vector{String},Vector{String},Union{Nothing, String},Union{Nothing, String},Union{Nothing, String},Vector{String},Bool,Bool,Bool,Bool,Bool,Union{Nothing, Bool},Bool,Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(start_session),))
    end
end   # time: 0.2499312
    Base.precompile(Tuple{typeof(download_remote_path),Any})   # time: 0.18280663
    Base.precompile(Tuple{typeof(sample),Any,Any,Vararg{Any}})   # time: 0.17623955
    isdefined(Banyan, Symbol("#141#147")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#141#147"))})   # time: 0.09118
    Base.precompile(Tuple{typeof(partitioned_computation),Function,Future,Location,Location,Function})   # time: 0.0821524
    Base.precompile(Tuple{Core.kwftype(typeof(Distributed)),Any,typeof(Distributed),Any})   # time: 0.07266555
    Base.precompile(Tuple{typeof(sqs_get_queue_with_retries),Dict{Symbol, Any},Vararg{Any}})   # time: 0.0666764
    Base.precompile(Tuple{typeof(Grouped),Future,Vector{String},Vector{Bool},Union{Nothing, Bool},Dict{Future, Union{Nothing, String}},Dict{Future, Union{Nothing, String}},Vector{Future}})   # time: 0.06459368
    Base.precompile(Tuple{typeof(orderinghash),AbstractString})   # time: 0.06285435
    Base.precompile(Tuple{Core.kwftype(typeof(pt)),NamedTuple{(:match, :on), Tuple{Future, String}},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}}})   # time: 0.045317233
    Base.precompile(Tuple{typeof(Value),Function})   # time: 0.042100452
    Base.precompile(Tuple{Core.kwftype(typeof(request_json)),NamedTuple{(:input, :method, :headers), Tuple{IOBuffer, String, Vector{Pair{String, String}}}},typeof(request_json),String})   # time: 0.041555937
    Base.precompile(Tuple{typeof(to_jl),RecordTaskRequest})   # time: 0.027162991
    let fbody = try __lookup_kwbody__(which(Grouped, (AbstractFuture,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Vector{String},Union{Nothing, Bool},Union{Nothing, Bool},Nothing,Nothing,Union{Nothing, AbstractFuture},typeof(Grouped),AbstractFuture,))
    end
end   # time: 0.023250869
    let fbody = try __lookup_kwbody__(which(Future, (Any,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Any,Type{Future},Any,))
    end
end   # time: 0.020540096
    Base.precompile(Tuple{typeof(Value),Int64})   # time: 0.020179976
    let fbody = try __lookup_kwbody__(which(get_clusters, (Any,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_clusters),Any,))
    end
end   # time: 0.01874336
    Base.precompile(Tuple{Core.kwftype(typeof(pt)),NamedTuple{(:match, :on), Tuple{Future, Vector{String}}},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}}})   # time: 0.016820444
    let fbody = try __lookup_kwbody__(which(pt, (Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Nothing,Vector{String},Vector{Future},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))
    end
end   # time: 0.013556061
    Base.precompile(Tuple{typeof(RemoteSource),Function,Any,Bool,Bool,Bool,Bool,Bool})   # time: 0.012268761
    Base.precompile(Tuple{Core.kwftype(typeof(Grouped)),Any,typeof(Grouped),AbstractFuture})   # time: 0.012203359
    Base.precompile(Tuple{Core.kwftype(typeof(Blocked)),Any,typeof(Blocked),AbstractFuture})   # time: 0.01076339
    Base.precompile(Tuple{Core.kwftype(typeof(partitioned_with)),NamedTuple{(:scaled, :keep_same_keys, :drifted, :modules), Tuple{Vector{AbstractFuture}, Bool, Bool, String}},typeof(partitioned_with),Function})   # time: 0.009349797
    Base.precompile(Tuple{Core.kwftype(typeof(run_with_retries)),NamedTuple{(:failure_message,), Tuple{String}},typeof(run_with_retries),Function,Dict{Symbol, Any},Vararg{Any}})   # time: 0.008550123
    Base.precompile(Tuple{Core.kwftype(typeof(partitioned_with)),NamedTuple{(:scaled, :grouped, :keys, :drifted, :modules), Tuple{Vector{AbstractFuture}, Vector{AbstractFuture}, Vector{String}, Bool, String}},typeof(partitioned_with),Function})   # time: 0.008497576
    let fbody = try __lookup_kwbody__(which(Grouped, (AbstractFuture,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Nothing,Union{Nothing, Bool},Union{Nothing, Bool},Nothing,Future,Union{Nothing, AbstractFuture},typeof(Grouped),AbstractFuture,))
    end
end   # time: 0.008060969
    Base.precompile(Tuple{typeof(sample),Any,Any})   # time: 0.007890783
    let fbody = try __lookup_kwbody__(which(Grouped, (AbstractFuture,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Nothing,Union{Nothing, Bool},Union{Nothing, Bool},Future,Nothing,Union{Nothing, AbstractFuture},typeof(Grouped),AbstractFuture,))
    end
end   # time: 0.007755828
    Base.precompile(Tuple{Type{DelayedTask}})   # time: 0.006598645
    Base.precompile(Tuple{Core.kwftype(typeof(configure_scheduling)),NamedTuple{(:name,), Tuple{String}},typeof(configure_scheduling)})   # time: 0.006280868
    Base.precompile(Tuple{Core.kwftype(typeof(partitioned_with)),NamedTuple{(:scaled,), Tuple{Future}},typeof(partitioned_with),Function})   # time: 0.005261271
    Base.precompile(Tuple{typeof(sample),Sample,Symbol,Vararg{Any}})   # time: 0.005224918
    Base.precompile(Tuple{Type{PartitionType},Pair{String, Nothing},Vararg{Union{PartitioningConstraint, Function, String, Pair{String}}}})   # time: 0.004824688
    Base.precompile(Tuple{Type{PartitionType},Pair{String, String},Vararg{Union{PartitioningConstraint, Function, String, Pair{String}}}})   # time: 0.00477251
    let fbody = try __lookup_kwbody__(which(Grouped, (AbstractFuture,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Vector{String},Union{Nothing, Bool},Union{Nothing, Bool},Future,Nothing,Union{Nothing, AbstractFuture},typeof(Grouped),AbstractFuture,))
    end
end   # time: 0.004535016
    let fbody = try __lookup_kwbody__(which(Grouped, (AbstractFuture,))) catch missing end
    if !ismissing(fbody)
        precompile(fbody, (Vector{String},Union{Nothing, Bool},Union{Nothing, Bool},Nothing,Future,Union{Nothing, AbstractFuture},typeof(Grouped),AbstractFuture,))
    end
end   # time: 0.003907398
    isdefined(Banyan, Symbol("#58#59")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#58#59")),Pair{String, Any}})   # time: 0.003704884
    Base.precompile(Tuple{Core.kwftype(typeof(Type)),NamedTuple{(:datatype,), Tuple{String}},Type{Future}})   # time: 0.002224462
    Base.precompile(Tuple{Core.kwftype(typeof(Scale)),NamedTuple{(:by, :relative_to), Tuple{Float64, Vector{Future}}},typeof(Scale),Future})   # time: 0.002147617
    Base.precompile(Tuple{Type{Session},String,String,String,Int64,Int64,String,String,Vector{String}})   # time: 0.001693318
    Base.precompile(Tuple{typeof(get_remote_source_cached),Any,Any,Any,Any,Any})   # time: 0.001624587
    Base.precompile(Tuple{typeof(setsample!),Future,Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.001528295
    Base.precompile(Tuple{Type{PartitionType},Pair{String, String},Vararg{Pair{String, String}}})   # time: 0.001504978
    isdefined(Banyan, Symbol("#187#188")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#187#188")),Downloads.Curl.Easy,NamedTuple{(:url, :method, :headers), Tuple{String, String, Vector{Pair{String, String}}}}})   # time: 0.001371738
    isdefined(Banyan, Symbol("#187#188")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#187#188")),Downloads.Curl.Easy,NamedTuple{(:url, :method, :headers), Tuple{String, Nothing, Vector{Pair{String, String}}}}})   # time: 0.001366879
    Base.precompile(Tuple{typeof(Reducing),Union{Expr, Function}})   # time: 0.00133871
    Base.precompile(Tuple{typeof(setsample!),Future,Int64})   # time: 0.001283355
    Base.precompile(Tuple{typeof(setsample!),Future,Symbol})   # time: 0.001151044
end
