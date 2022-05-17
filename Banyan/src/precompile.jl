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
    Base.precompile(Tuple{Core.kwftype(typeof(start_session)),NamedTuple{(:cluster_name, :nworkers, :sample_rate, :print_logs, :url, :branch, :directory, :dev_paths, :release_resources_after, :force_pull, :force_sync, :force_install, :store_logs_on_cluster), Tuple{String, Int64, Int64, Bool, String, String, String, Vector{String}, Int64, Bool, Bool, Bool, Bool}},typeof(start_session)})   # time: 2.1705344
    let fbody = try __lookup_kwbody__(which(pt, (Any,Vararg{Any},))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Union{Nothing, Future},Union{String, Vector{String}},Vector{Future},typeof(pt),Any,Vararg{Any},))
        end
    end   # time: 0.13578409
    Base.precompile(Tuple{Core.kwftype(typeof(start_session)),Any,typeof(start_session)})   # time: 0.07899095
    isdefined(Banyan, Symbol("#13#14")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#13#14"))})   # time: 0.056121398
    Base.precompile(Tuple{Type{Future},Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.047179397
    Base.precompile(Tuple{typeof(to_jl),RecordTaskRequest})   # time: 0.03714645
    Base.precompile(Tuple{Type{Future},Function})   # time: 0.03367833
    Base.precompile(Tuple{typeof(make_blocked_pts),Future,Vector{Int64},Vector{Bool},Vector{Future},Vector{Future},Vector{Future}})   # time: 0.02814891
    Base.precompile(Tuple{typeof(orderinghash),String})   # time: 0.025676288
    Base.precompile(Tuple{Core.kwftype(typeof(Type)),Any,Type{Session},String,String,String,Int64,Int64,String,String,Vector{String},Bool,Bool})   # time: 0.013642028
    let fbody = try __lookup_kwbody__(which(partitioned_with, (Function,Vector{Future},))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Nothing,Nothing,Bool,Bool,Vector{String},DataType,typeof(partitioned_with),Function,Vector{Future},))
        end
    end   # time: 0.013607993
    Base.precompile(Tuple{typeof(partitioned_computation_concrete),Function,Future,Location,Location,Function})   # time: 0.008437464
    Base.precompile(Tuple{typeof(pt_for_replicated),Vector{Future}})   # time: 0.008295843
    Base.precompile(Tuple{Core.kwftype(typeof(configure_scheduling)),NamedTuple{(:name,), Tuple{String}},typeof(configure_scheduling)})   # time: 0.005944979
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}},Union{PartitioningConstraint, Function, String, Pair{String}}})   # time: 0.005579519
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}}})   # time: 0.00484481
    isdefined(Banyan, Symbol("#76#77")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#76#77")),Future})   # time: 0.00397727
    let fbody = try __lookup_kwbody__(which(partitioned_with, (Function,Vector{Future},))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Vector{String},Nothing,Bool,Bool,Vector{String},DataType,typeof(partitioned_with),Function,Vector{Future},))
        end
    end   # time: 0.003753834
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}},Vararg{Union{PartitioningConstraint, Function, String, Pair{String}}}})   # time: 0.00370984
    Base.precompile(Tuple{Type{DelayedTask}})   # time: 0.003556886
    Base.precompile(Tuple{Core.kwftype(typeof(Type)),NamedTuple{(:datatype,), Tuple{String}},Type{Future}})   # time: 0.003504575
    Base.precompile(Tuple{typeof(merge_pts!),PartitionType,PartitionType,Vector{PartitionType}})   # time: 0.003056493
    Base.precompile(Tuple{typeof(get_key_for_sample_computation_cache),SampleComputationCache,UInt64})   # time: 0.002846733
    Base.precompile(Tuple{Type{Sample},Any,Int64})   # time: 0.001376823
    let fbody = try __lookup_kwbody__(which(get_cluster, (String,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_cluster),String,))
        end
    end   # time: 0.001287449
    Base.precompile(Tuple{typeof(_get_factor),Float64,Tuple{Nothing, Int64},Tuple{Nothing, Int64}})   # time: 0.001243914
    for SV in [Int64, String, Function, Symbol, Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}]
        Base.precompile(Tuple{typeof(setsample!),Future,SV})   # time: 0.001174972
    end

    # Additional precompile statements

    # annotation.jl
    precompile(keep_sample_rate, (Future, Base.Vector{Future}))
    precompile(keep_sample_rate, (Future, Future))
    for K in [String, Int64]
        precompile(_get_new_p_groupingkeys!, (Future, Sample, Vector{Future}, Bool, Vector{K}, Vector{Tuple{Future,K,Future,K}}))
        precompile(keep_all_sample_keys, (Base.Vector{Future}, Bool, Vector{Tuple{Future,K,Future,K}}))
        precompile(
            keep_all_sample_keys_renamed,
            (
                Tuple{Future,Sample,Vector{K}},
                Tuple{Future,Sample,Vector{K}},
                Vector{Tuple{Future,K,Future,K}}
            )
        )
        precompile(
            keep_all_sample_keys_renamed,
            (
                Future,
                Future,
                Type{K},
                Vector{Tuple{Future,K,Future,K}}
            )
        )
        precompile(
            keep_sample_keys_named,
            (
                Vector{Tuple{Future,Vector{K}}},
                Bool,
                Vector{Tuple{Future,K,Future,K}}
            )
        )
        precompile(keep_sample_keys, (Vector{K}, Vector{Future}, Bool, Vector{Tuple{Future,K,Future,K}}))
        precompile(apply_keeping_same_statistics, (Vector{Tuple{Future,K,Future,K}},))
        precompile(apply_partitioned_using_func,(PartitionedUsingFunc{K},))
        precompile(
            partitioned_with,
            (
                Function,
                # Memory usage, sampling
                # `scaled` is the set of futures with memory usage that can potentially be
                # scaled to larger sizes if the amount of data at a location changes.
                # Non-scaled data has fixed memory usage regardless of its sample rate.
                Vector{Future},
                Bool,
                Vector{PartitioningConstraint},
                Vector{PartitioningConstraint},
                # Keys (not relevant if you never use grouped partitioning).
                Vector{Future},
                Bool,
                Vector{K},
                Vector{Tuple{Future,Vector{K}}},
                Bool,
                # Asserts that output has a unique partitioning compared to inputs
                # (not relevant if you never have unbalanced partitioning)
                Bool,
                # For generating import statements
                Vector{String}
            )
        )
        Base.precompile(Tuple{typeof(GroupedBy),K,Bool})   # time: 0.001962668
        Base.precompile(Tuple{typeof(make_grouped_pt),Future,K,Vector{Future}})   # time: 0.002876798
        Base.precompile(Tuple{typeof(_partitioned_with),Function,Vector{Future},Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Vector{K},Vector{Tuple{Future, Vector{K}}},Bool,Bool,Vector{String}})   # time: 0.00298401
    end
    precompile(apply_partitioned_using_func_for_sample_rates, (Vector{Future}, Bool, Vector{Future}))
    precompile(
        get_inputs_and_outputs,
        (
            IdDict{Future,Future},
            Vector{Future},
            Bool,
            Vector{Future}
        )
    )
    precompile(pt_partition_type_composition, (Future, PartitionTypeComposition, Vector{Future}, Vector{String}, Vector{Future}))
    precompile(pt_partition_type, (PartitionType, Vector{Future}, Vector{Future}, Vector{String}, Vector{Future}))
    precompile(pt_partition_type, (PartitionTypeComposition, Vector{Future}, Vector{Future}, Vector{String}, Vector{Future}))
    precompile(pt_partition_type, (Base.Vector{PartitionType}, Vector{Future}, Vector{Future}, Vector{String}, Vector{Future}))
    precompile(pt_for_replicated, (Vector{Future},))
    precompile(apply_mutation, (Future, Future))
    precompile(get_splatted_futures, (Vector{Union{Future,Vector{Future}}},))
    precompile(reassign_futures, (Vector{Union{Future,Vector{Future}}}, Vector{Union{Any,Vector{Any}}}))
    precompile(finish_partitioned_code_region, (Vector{Future},))
    precompile(get_splatted_futures, (Vector{Union{Future,Vector{Future}}},))
    precompile(
        prepare_task_for_partitioned_code_region,
        (
            Vector{Union{Future,Vector{Future}}},
            Vector{String},
            Vector{Future},
            String
        )
    )
    precompile(partitioned_code_region, (Vector{Expr}, Vector{String}, Expr, Vector{Expr}))
    precompile(apply_default_constraints!, (PartitionAnnotation,))
    precompile(duplicated_constraints_for_batching, (PartitioningConstraints, PartitionAnnotation))
    precompile(duplicate_for_batching!, (PartitionAnnotation,))

    # futures.jl
    precompile(create_future, (String, Any, ValueId, Bool, Bool))
    precompile(create_future, (String, Nothing, ValueId, Bool, Bool))

    # partitions.jl
    precompile(merge_pts!, (PartitionType, PartitionType, Vector{PartitionType}))

    # requests.jl
    precompile(_partitioned_computation_concrete, (Future, Location, Location, Dict{SessionId,Session}, SessionId, Session, ResourceId))

    # utils_pfs.jl
    precompile(getpath, (String,))
    OHTypes = [Base.Vector{UInt8}, Base.Vector{Int64}, Base.Vector{Float64}, Base.Vector{Dates.DateTime}]
    for OH in OHTypes
        precompile(get_divisions, (Vector{Tuple{OH, OH}}, Int64))
        precompile(get_oh_partition_idx_from_divisions, (OH, Vector{Vector{Tuple{OH,OH}}}, Bool, Bool))
        precompile(get_all_divisions, (Base.Vector{OH}, Int64))
        Base.precompile(Tuple{typeof(_minimum),Vector{OH}})   # time: 0.003270399
        Base.precompile(Tuple{typeof(_maximum),Vector{OH}})
        Base.precompile(Tuple{typeof(get_all_divisions),Vector{OH},Int64})   # time: 0.005706646
    end

    # sessions.jl
    precompile(
        start_session,
        (
            String,
            Int64,
            Integer,
            Bool,
            Bool,
            Bool,
            Bool,
            Int64,
            String,
            Vector{String},
            Vector{String},
            Bool,
            Vector{String},
            Bool,
            Vector{String},
            # We currently can't use modules that require GUI
            Vector{String},
            String,
            String,
            String,
            Vector{String},
            Bool,
            Bool,
            Bool,
            Bool,
            Bool,
            Bool,
            Bool,
            Bool,
            Dict{String,Session},
        )
    )
    precompile(_partitioned_computation_concrete, (Future, Location, Dict{SessionId,Session}, SessionId, Session, ResourceId))

    # pt_lib_constructors.jl
    precompile(get_factor_for_blocked, (Sample, Vector{Future}, Bool))
    precompile(
        make_blocked_pt,
        (
            Future,
            Int64,
        Bool,
            Vector{Future},
            Vector{Future},
            Vector{Future},
        )
    )
    for K in [Int64, String]
        precompile(
            make_grouped_pt,
            (
                Future,
                K,
                Vector{Future},
            )
        )
    end
    for K in [String, Int64]
        precompile(sample_for_grouping, (Future, Vector{K}))
        precompile(make_grouped_pt, (Future, K, Vector{Future}))
    end

    # locations.jl
    precompile(get_cached_location, (String, Bool, Bool))
    precompile(cache_location, (String, Location, Bool, Bool))
    precompile(sample_from_range, (UnitRange{Int64}, Int64))

    # utils.jl, utils_s3fs.jl
    precompile(load_toml, (String,))
    precompile(load_toml, (Vector{String},))
    precompile(load_file, (String,))
    precompile(download_remote_path, (String,))
    precompile(download_remote_s3_path, (String,))
    Base.precompile(Tuple{typeof(sqs_get_queue_with_retries),Dict{Symbol, Any},Vararg{Any}})   # time: 0.24037404
    precompile(to_jl_value_contents, (Function,))

    # futures.jl
    precompile(create_new_future, (Location, Future, String))
    precompile(create_future_from_existing, (Future, Function))

    # # StaticArrays.jl
    # Base.precompile(Tuple{typeof(convert),Type{Vector{UInt8}},Vector{UInt8}})   # time: 0.16227919
    # # Base.precompile(Tuple{typeof(Base.cconvert),Type{Ptr{Any}},SArray})   # time: 0.014966334
    # Base.precompile(Tuple{typeof(Base.cconvert),Type{Ptr{Any}},FieldArray})   # time: 0.013368279
    # Base.precompile(Tuple{typeof(axes),SVector{32, UInt8}})   # time: 0.001536017

    # # HTTP.ExceptionRequest.jl
    # Base.precompile(Tuple{Core.kwftype(typeof(request)),NamedTuple{(:iofunction, :verbose, :require_ssl_verification), Tuple{Nothing, Int64, Bool}},typeof(request),Type{ExceptionLayer{HTTP.ConnectionRequest.ConnectionPoolLayer{HTTP.StreamRequest.StreamLayer{Union{}}}}},HTTP.URIs.URI,HTTP.Messages.Request,String})   # time: 1.080406

    # # Parsers.jl
    # Base.precompile(Tuple{typeof(tryparse),Type{Float64},SubString{String},Options,Int64,Int64})   # time: 1.3334063

    # # CSV.jl
    # Base.precompile(Tuple{typeof(iterate),Rows{Vector{UInt8}, Tuple{}, PosLen, PosLenString}})   # time: 0.12239744
    # Base.precompile(Tuple{typeof(makepooled2!),Column,Type{String15},Dict{Union{Missing, String15}, UInt32},Vector{UInt32}})   # time: 0.10618292

    # sessions = get_sessions_dict()
    # set_session("-1")
    # sessions[get_session_id()] = Session(
    #     "some_cluster",
    #     "some_cluster_id",
    #     "some_resource_id",
    #     4,
    #     4,
    # )
    # for (K, key) in [(String, ""), (Int64, 1)]
    #     t = DelayedTask()
    #     set_task(t)
    #     in = Future()
    #     b = Future(in)
    #     out = Future()
    #     t.inputs = Future[in]
    #     t.outputs = Future[out]
    #     mutated(out)
    #     t.scaled = Future[in, out]

    #     apply_partitioned_using_func(
    #         PartitionedUsingFunc{K}(
    #             true,
    #             Future[in, out],
    #             false,
    #             K[key],
    #             Dict{Future,Vector{K}}(),
    #             false,
    #             false,
    #             false
    #         )
    #     )
    # end
end