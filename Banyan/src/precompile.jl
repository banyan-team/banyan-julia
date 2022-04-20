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
    # Base.precompile(Tuple{Core.kwftype(typeof(start_session)),NamedTuple{(:cluster_name, :nworkers, :sample_rate, :print_logs, :url, :branch, :directory, :dev_paths, :release_resources_after, :force_pull, :force_sync, :force_install, :store_logs_on_cluster), Tuple{String, Int64, Int64, Bool, String, String, String, Vector{String}, Int64, Bool, Bool, Bool, Bool}},typeof(start_session)})   # time: 2.717487
    # Base.precompile(Tuple{typeof(compute),Future})   # time: 1.4883087
    precompile(compute, (Future,))
    # Base.precompile(Tuple{typeof(partitioned_computation),Function,Future,Location,Location,Function})   # time: 0.1165763
    precompile(partitioned_computation_concrete, (Function,Future,Location,Location,Function))
    # Base.precompile(Tuple{Core.kwftype(typeof(Grouped)),NamedTuple{(:balanced, :filtered_from, :filtered_to, :scaled_by_same_as), Tuple{Bool, Nothing, Future, Nothing}},typeof(Grouped),Future})   # time: 0.06890887
    # Base.precompile(Tuple{Core.kwftype(typeof(start_session)),Any,typeof(start_session)})   # time: 0.068669505
    let fbody = try __lookup_kwbody__(which(pt, (Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Future,String,Vector{Future},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))
        end
    end   # time: 0.04146457
    Base.precompile(Tuple{Type{Session},String,String,String,Int64,Int64,Any,Any,Any})   # time: 0.02856741
    # let fbody = try __lookup_kwbody__(which(get_cluster, (String,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_cluster),String,))
    #     end
    # end   # time: 0.027583364
    precompile(get_cluster_status, (String,))
    # let fbody = try __lookup_kwbody__(which(get_clusters, (Any,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_clusters),Any,))
    #     end
    # end   # time: 0.026738392
    precompile(_get_clusters, (String,))
    precompile(get_cluster, (String,))
    # Base.precompile(Tuple{typeof(sqs_receive_message_with_long_polling),Dict{Symbol, Any}})   # time: 0.025069334
    precompile(sqs_receive_message_with_long_polling, (Dict{Symbol,Any},))
    # Base.precompile(Tuple{typeof(to_jl),RecordTaskRequest})   # time: 0.021836402
    for Request in [RecordTaskRequest, RecordLocationRequest, DestroyRequest]
        precompile(to_jl, (Request,))
    end
    # let fbody = try __lookup_kwbody__(which(get_cluster_status, (String,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_cluster_status),String,))
    #     end
    # end   # time: 0.015606671
    let fbody = try __lookup_kwbody__(which(Blocked, (Future,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Colon,Bool,Nothing,Future,Nothing,typeof(Blocked),Future,))
        end
    end   # time: 0.01459359
    # let fbody = try __lookup_kwbody__(which(wait_for_cluster, (String,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(wait_for_cluster),String,))
    #     end
    # end   # time: 0.012923986
    precompile(_wait_for_cluster, (String,))
    # Base.precompile(Tuple{Type{Future},Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.012588012
    # let fbody = try __lookup_kwbody__(which(pt, (Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Future,Vector{String},Vector{Future},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))
    #     end
    # end   # time: 0.011352393
    # let fbody = try __lookup_kwbody__(which(pt, (Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Nothing,Vector{String},Vector{Future},typeof(pt),Future,Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))
    #     end
    # end   # time: 0.010971302
    # Base.precompile(Tuple{Core.kwftype(typeof(Grouped)),NamedTuple{(:balanced, :filtered_from, :filtered_to, :scaled_by_same_as), Tuple{Bool, Future, Nothing, Nothing}},typeof(Grouped),Future})   # time: 0.008728958
    Base.precompile(Tuple{Type{DelayedTask}})   # time: 0.008181804
    Base.precompile(Tuple{Core.kwftype(typeof(partitioned_with)),NamedTuple{(:scaled,), Tuple{Future}},typeof(partitioned_with),Function})   # time: 0.007249045
    Base.precompile(Tuple{Core.kwftype(typeof(run_with_retries)),NamedTuple{(:failure_message,), Tuple{String}},typeof(run_with_retries),Function,Dict{Symbol, Any},Vararg{Any}})   # time: 0.006951094
    Base.precompile(Tuple{typeof(ExactSample),Function})   # time: 0.006702023
    # Base.precompile(Tuple{Core.kwftype(typeof(configure_scheduling)),NamedTuple{(:name,), Tuple{String}},typeof(configure_scheduling)})   # time: 0.00666435
    # let fbody = try __lookup_kwbody__(which(Grouped, (Future,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Vector{String},Bool,Nothing,Nothing,Future,Nothing,typeof(Grouped),Future,))
    #     end
    # end   # time: 0.004773759
    # let fbody = try __lookup_kwbody__(which(Grouped, (Future,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Vector{String},Bool,Nothing,Future,Nothing,Nothing,typeof(Grouped),Future,))
    #     end
    # end   # time: 0.004074963
    # Base.precompile(Tuple{typeof(RemoteSource),Function,Any,Bool,Bool,Bool,Bool,Bool})   # time: 0.004018768
    # isdefined(Banyan, Symbol("#85#86")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#85#86")),Future})   # time: 0.003776841
    # Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}},Vararg{Union{PartitioningConstraint, Function, String, Pair{String}}}})   # time: 0.003769399
    # isdefined(Banyan, Symbol("#89#90")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#89#90")),Future})   # time: 0.003759113
    # isdefined(Banyan, Symbol("#54#55")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#54#55")),Pair{String, Any}})   # time: 0.003541598
    # Base.precompile(Tuple{Type{Future},Any})   # time: 0.002759026
    # Base.precompile(Tuple{typeof(setsample!),Future,Symbol})   # time: 0.002529208
    # Base.precompile(Tuple{Core.kwftype(typeof(Type)),NamedTuple{(:datatype,), Tuple{String}},Type{Future}})   # time: 0.002243466
    # Base.precompile(Tuple{typeof(setsample!),Future,Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.002113298
    # Base.precompile(Tuple{typeof(setsample!),Future,Function})   # time: 0.001720393
    # Base.precompile(Tuple{Core.kwftype(typeof(Scale)),NamedTuple{(:by,), Tuple{Float64}},typeof(Scale),Future})   # time: 0.001678438
    # isdefined(Banyan, Symbol("#131#132")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#131#132")),Downloads.Curl.Easy,NamedTuple{(:url, :method, :headers), Tuple{String, String, Vector{Pair{String, String}}}}})   # time: 0.001537199
    # let fbody = try __lookup_kwbody__(which(get_cluster_s3_bucket_arn, (Any,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_cluster_s3_bucket_arn),Any,))
    #     end
    # end   # time: 0.001492919
    # Base.precompile(Tuple{typeof(get_remote_source_cached),Any,Any,Any,Any,Any})   # time: 0.001470609
    precompile(get_remote_source_cached, (Any,Any,Any,Any,Any))
    # isdefined(Banyan, Symbol("#131#132")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#131#132")),Downloads.Curl.Easy,NamedTuple{(:url, :method, :headers), Tuple{String, Nothing, Vector{Pair{String, String}}}}})   # time: 0.001462179
    # isdefined(Banyan, Symbol("#45#46")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#45#46")),Any})   # time: 0.001335263
    # Base.precompile(Tuple{Type{Sample},Any,Int64})   # time: 0.001243521
    # Base.precompile(Tuple{typeof(setsample!),Future,Int64})   # time: 0.001141056
    # Base.precompile(Tuple{typeof(Value),Any})   # time: 0.00111847
    # let fbody = try __lookup_kwbody__(which(Blocked, (Future,))) catch missing end
    #     if !ismissing(fbody)
    #         precompile(fbody, (Colon,Bool,Future,Nothing,Nothing,typeof(Blocked),Future,))
    #     end
    # end   # time: 0.001071065

    # New precompile statements

    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    Base.precompile(Tuple{Core.kwftype(typeof(start_session)),NamedTuple{(:cluster_name, :nworkers, :sample_rate, :print_logs, :url, :branch, :directory, :dev_paths, :release_resources_after, :force_pull, :force_sync, :force_install, :store_logs_on_cluster), Tuple{String, Int64, Int64, Bool, String, String, String, Vector{String}, Int64, Bool, Bool, Bool, Bool}},typeof(start_session)})   # time: 2.5331068
    Base.precompile(Tuple{typeof(compute),Future})   # time: 0.9542997
    let fbody = try __lookup_kwbody__(which(pt, (Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}},Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Union{Nothing, AbstractFuture},Union{String, Vector{String}},Vector{<:AbstractFuture},typeof(pt),Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}},Vararg{Union{AbstractFuture, PartitionType, PartitionTypeComposition, Vector{PartitionType}}},))
        end
    end   # time: 0.31887534
    Base.precompile(Tuple{Type{Sample},Any,Int64})   # time: 0.2173959
    # Base.precompile(Tuple{typeof(partitioned_computation),Function,Future,Location,Location,Function})   # time: 0.12135102
    let fbody = try __lookup_kwbody__(which(partitioned_with, (Function,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Nothing,Nothing,Bool,Bool,String,DataType,typeof(partitioned_with),Function,))
        end
    end   # time: 0.119159184
    Base.precompile(Tuple{Core.kwftype(typeof(start_session)),Any,typeof(start_session)})   # time: 0.07294108
    isdefined(Banyan, Symbol("#12#13")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#12#13"))})   # time: 0.05842767
    Base.precompile(Tuple{typeof(load_toml),String})   # time: 0.05007964
    Base.precompile(Tuple{Type{Future},Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.046877228
    Base.precompile(Tuple{Type{Future},Function})   # time: 0.027417323
    Base.precompile(Tuple{Core.kwftype(typeof(Blocked)),NamedTuple{(:balanced, :filtered_from, :filtered_to, :scaled_by_same_as), _A} where _A<:Tuple{Bool, Union{Nothing, Future}, Union{Nothing, Future}, Nothing},typeof(Blocked),Future})   # time: 0.019588284
    Base.precompile(Tuple{typeof(to_jl_value),Vector{Tuple{Vector{UInt8}, Vector{UInt8}}}})   # time: 0.017036038
    Base.precompile(Tuple{typeof(sample_for_grouping),Future,Vector{String}})   # time: 0.013155817
    Base.precompile(Tuple{typeof(to_jl_value),Vector})   # time: 0.010911949
    Base.precompile(Tuple{Type{Future},Symbol})   # time: 0.007323756
    Base.precompile(Tuple{typeof(apply_partitioned_using_func),PartitionedUsingFunc{Int64}})   # time: 0.005683017
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}},Union{PartitioningConstraint, Function, String, Pair{String}}})   # time: 0.005610858
    Base.precompile(Tuple{Core.kwftype(typeof(configure_scheduling)),NamedTuple{(:name,), Tuple{String}},typeof(configure_scheduling)})   # time: 0.005372673
    Base.precompile(Tuple{typeof(apply_partitioned_using_func),PartitionedUsingFunc{String}})   # time: 0.00530907
    Base.precompile(Tuple{Core.kwftype(typeof(partitioned_with)),NamedTuple{(:scaled,), Tuple{Future}},typeof(partitioned_with),Function})   # time: 0.005217431
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}}})   # time: 0.004976139
    Base.precompile(Tuple{typeof(RemoteSource),Function,Any,Bool,Bool,Bool,Bool,Bool})   # time: 0.003924375
    Base.precompile(Tuple{Type{PartitionType},Union{PartitioningConstraint, Function, String, Pair{String}},Vararg{Union{PartitioningConstraint, Function, String, Pair{String}}}})   # time: 0.003795865
    Base.precompile(Tuple{Core.kwftype(typeof(Blocked)),NamedTuple{(:balanced, :filtered_from, :filtered_to, :scaled_by_same_as), Tuple{Bool, Future, Nothing, Nothing}},typeof(Blocked),Future})   # time: 0.003686234
    let fbody = try __lookup_kwbody__(which(partitioned_with, (Function,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Vector{String},Nothing,Bool,Bool,String,DataType,typeof(partitioned_with),Function,))
        end
    end   # time: 0.003673407
    isdefined(Banyan, Symbol("#77#78")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#77#78")),Future})   # time: 0.003634369
    Base.precompile(Tuple{Core.kwftype(typeof(Blocked)),NamedTuple{(:balanced, :filtered_from, :filtered_to, :scaled_by_same_as), Tuple{Bool, Nothing, Future, Nothing}},typeof(Blocked),Future})   # time: 0.003485579
    Base.precompile(Tuple{Type{DelayedTask}})   # time: 0.003402792
    Base.precompile(Tuple{typeof(_partitioned_with),Function,Vector{Future},Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Vector{String},Vector{Tuple{Future, Vector{String}}},Bool,Bool,Vector{String}})   # time: 0.003231375
    isdefined(Banyan, Symbol("#73#74")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#73#74")),Future})   # time: 0.003163366
    Base.precompile(Tuple{typeof(noscale),Future})   # time: 0.002822761
    Base.precompile(Tuple{Core.kwftype(typeof(Type)),NamedTuple{(:datatype,), Tuple{String}},Type{Future}})   # time: 0.002799369
    Base.precompile(Tuple{typeof(GroupedBy),String,Bool})   # time: 0.002383662
    Base.precompile(Tuple{typeof(setsample!),Future,Function})   # time: 0.001718602
    Base.precompile(Tuple{typeof(setsample!),Future,Base.Pairs{Symbol, Union{}, Tuple{}, NamedTuple{(), Tuple{}}}})   # time: 0.001511772
    Base.precompile(Tuple{typeof(setsample!),Future,Symbol})   # time: 0.001438182
    Base.precompile(Tuple{typeof(setsample!),Future,Int64})   # time: 0.001398406
    let fbody = try __lookup_kwbody__(which(partitioned_with, (Function,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Future,Bool,Vector{PartitioningConstraint},Vector{PartitioningConstraint},Vector{Future},Bool,Nothing,Nothing,Bool,Bool,Vector{String},DataType,typeof(partitioned_with),Function,))
        end
    end   # time: 0.001386771
    isdefined(Banyan, Symbol("#109#110")) && Base.precompile(Tuple{getfield(Banyan, Symbol("#109#110")),Downloads.Curl.Easy,NamedTuple{(:url, :method, :headers), Tuple{String, String, Vector{Pair{String, String}}}}})   # time: 0.001313547
    let fbody = try __lookup_kwbody__(which(get_cluster, (String,))) catch missing end
        if !ismissing(fbody)
            precompile(fbody, (Base.Pairs{Symbol, V, Tuple{Vararg{Symbol, N}}, NamedTuple{names, T}} where {V, N, names, T<:Tuple{Vararg{Any, N}}},typeof(get_cluster),String,))
        end
    end   # time: 0.001132337
    Base.precompile(Tuple{typeof(get_remote_source_cached),Any,Any,Any,Any,Any})   # time: 0.001114128

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
    precompile(getpath, (String, MPI.Comm))
    for OH in [Vector{UInt8}, Vector{Int64}]
        precompile(get_divisions, (Vector{Tuple{OH, OH}}, Int64))
        precompile(get_oh_partition_idx_from_divisions, (OH, Vector{Vector{Tuple{OH,OH}}}, Bool, Bool))
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
    end

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

    #     @time begin
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
    #     println("Time for apply_partitioned_using_func in precompilation")
    #     end
    # end
end