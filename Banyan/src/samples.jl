###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

ExactSample(value::Any) = Sample(value, sample_memory_usage(value), 1)
ExactSample(value::Any, memory_usage::Int64) = Sample(value, memory_usage, 1)

function setsample!(fut::Future, value::Any)
    s::Sample = get_location(fut).sample
    memory_usage::Int64 = sample_memory_usage(value)
    rate::Int64 = s.rate
    s.value = value
    s.memory_usage = convert(Int64, round(memory_usage / rate))::Int64
    s.objectid = objectid(value)
end

# sample* functions always return a concrete value or a dict with properties.
# To get a `Sample`, access the property.

# TODO: Lazily compute samples by storing sample computation in a DAG if its
# getting too expensive
sample(fut::AbstractFuture) = get_location(convert(Future, fut)::Future).sample.value
sample(fut::Future) = get_location(fut).sample.value

# sample(fut::AbstractFuture, propertykeys...) = sample(convert(Future, fut)::Future, propertykeys...)
# sample(fut::Future, propertykeys...) = sample(get_location(fut).sample, propertykeys...)
# function sample(s::Sample, propertykeys...)
#     properties = s.properties
#     for (i, propertykey) in enumerate(propertykeys)
#         properties = get!(
#             properties,
#             propertykey,
#             if i < length(propertykeys)
#                 Dict{String,Any}()
#             else
#                 sample(s.value, propertykeys...)
#             end
#         )
#     end
#     properties
# end

# # TODO: Finish this file to use sample!() everywhere

# setsample!(fut::AbstractFuture, value) = setsample!(convert(Future, fut)::Future, value)
# setsample!(fut::Future, value) = setsample!(get_location(fut).sample, value)
# function setsample!(sample::Sample, value)
#     sample.value = value
# end

# setsample!(fut::AbstractFuture, propertykeys...) = setsample!(convert(Future, fut)::Future, propertykeys...)
# setsample!(fut::Future, propertykeys...) = setsample!(get_location(fut).sample, propertykeys...)
# function setsample!(sample::Sample, propertykeys...)
#     if length(propertykeys) == 1
#         setsample!(sample, first(propertykeys))
#     else
#         properties = sample.properties
#         propertyvalue = last(propertykeys)
#         propertykeys = propertykeys[1:end-1]
#         for (i, propertykey) in enumerate(propertykeys)
#             if i < length(propertykeys)
#                 properties = get!(prsetsample!(operties, propertykey, Dict{String,Any}())
#             end
#         end
#         properties[last(propertykeys)] = propertyvalue
#     end
# end

# TODO: For futures with locations like Size, "scale up" the computed sample
# for a useful approximation of things like length of an array

####################################################################
# AbstractSample to be implemented by anything that can be sampled #
####################################################################

# NOTE: We use strings for things that will be serialized to JSON and symbols
# for everything else

# NOTE: We use upper-camel-case for user-facing names (like names of PTs) and
# all-caps-snake-case for anything internal (like names of constraints)

# The purpose of the `sample` function is to allow for computing various
# properties of the sample by property key name instead of an explicit
# function call. This makes it easier for the `sample` and `setsample!`
# functions for `Future`s to compute and cache samples.

# sample(as::Any, properties...) =
#     if length(properties) <= 2 && first(properties) == :statistics
#         # If the statistic is not cached in `Sample.properties`, then we just
#         # return an empty dictionary
#         Dict{String,Any}()
#     elseif length(properties) == 1
#         if first(properties) == :memory_usage
#             sample_memory_usage(as)::Int64
#         elseif first(properties) == :rate
#             # This is the default but the `Sample` constructor overrides this
#             # before-hand to allow some samples to be "exact" with a sample
#             # rate of 1
#             get_session().sample_rate
#         elseif first(properties) == :keys
#             sample_keys(as)
#         elseif first(properties) == :axes
#             sample_axes(as)
#         elseif first(properties) == :groupingkeys
#             # This is just the initial value for grouping keys. Calls to
#             # `keep_*` functions will expand it.
#             []
#         else
#             # println(typeof(as))
#             # println(typeof(as) <: Any)
#             throw(ArgumentError("Invalid sample properties: $properties"))
#         end
#     elseif length(properties) == 3 && first(properties) == :statistics
#         key = properties[2]
#         query = properties[3]
#         if query == :max_ngroups
#             sample_max_ngroups(as, key)
#         elseif query == :divisions
#             sample_divisions(as, key)
#         elseif query == :min
#             sample_min(as, key)
#         elseif query == :max
#             sample_max(as, key)
#         else
#             throw(ArgumentError("Invalid sample properties: $properties"))
#         end
#     elseif length(properties) == 5 && first(properties) == :statistics
#         key, query, minvalue, maxvalue = properties[2:end]
#         if query == :percentile
#             sample_percentile(as, key, minvalue, maxvalue)
#         else
#             throw(ArgumentError("Invalid sample properties: $properties"))
#         end
#     else
#         throw(ArgumentError("Invalid sample properties: $properties"))
#     end

# Implementation error 
impl_error(fn_name, as) = error("$fn_name not implemented for $(typeof(as))")

# Functions to implement for Any (e.g., for DataFrame or
# Array)
sample_memory_usage(as::Any)::Int64 = total_memory_usage(as)
sample_axes(as::Any)::Vector{Int64} = impl_error("sample_axes", as)
sample_keys(as::Any) = impl_error("sample_keys", as)
sample_divisions(as::Any, key) = impl_error("sample_divisions", as)
sample_percentile(as::Any, key, minvalue, maxvalue)::Float64 = impl_error("sample_percentile", as)
sample_max_ngroups(as::Any, key)::Int64 = impl_error("sample_max_ngroups", as)
sample_min(as::Any, key) = impl_error("sample_min", as)
sample_max(as::Any, key) = impl_error("sample_max", as)

const NOTHING_SAMPLE = Sample(nothing, -1, -1)

Base.isnothing(s::Sample) = s.rate == -1

# Caching samples with same statistics

# A sample with memoized statistics for 
# Must be mutable so that the Future finalizer runs
mutable struct SampleForGrouping{T,K}
    future::Future
    # samples only for the keys that could be used for grouping
    sample::T
    # keys
    keys::Vector{K}
    axes::Vector{Int64}
end

# Note that filtered_to's sample might be a vector

# This functions is for retrieving a sample of a future with same
# statistics properties with this key. Note that this function is not
# type-stable and its return type isn't knowable so it _will_ result
# in type inference. The best way to deal with that is to make sure to pass
# the result of calling this (or even the other `sample` functions) into a
# separate function to actually process its statistics. This creates a function
# barrier and type inference will still happen at run time but it will only
# happen once.
function sample_for_grouping(f::Future, keys::Vector{K}, f_sample::T)::SampleForGrouping{T,K} where {T,K}
    # keys::Vector{K} = keys[1:min(8,end)]
    # global same_statistics
    # f_sample 
    # # TODO: In the future we may need to allow grouping keys to be shared between futures
    # # with different sample types but same grouping keys and same statistics.
    # # For example, we might do an array resizing operation and keep some dimensions
    # # that can still be having the same grouping keys. If we do this, we should immediately
    # # ge ta type error here and will have to change the T to an Any.
    # res::Dict{K,T} = Dict{K,T}()
    # for key in keys
    #     f_map_key::Tuple{ValueId,K} = (f.value_id, key)
    #     s::T = haskey(same_statistics, f_map_key) ? same_statistics[f_map_key]::T : f_sample
    #     res[key] = s
    # end
    SampleForGrouping{T,K}(f, f_sample, keys, sample_axes(f_sample))
end
function sample_for_grouping(f::Future, keys::Vector{K}) where {K} sample_for_grouping(f, keys, sample(f)) end
function sample_for_grouping(f::Future, key::K) where {K} sample_for_grouping(f, K[key]) end
function sample_for_grouping(f::Future, ::Type{K}) where {K} sample_for_grouping(f, (get_location(f).sample.groupingkeys)::Vector{K}) end
sample_for_grouping(f::Future) = sample_for_grouping(f, Int64)

# struct SampleComputationCache
#     floats::Dict{UInt,Float64}
#     ints::Dict{UInt,Int}
#     anys::Dict{UInt,Any}
# end

# global sample_computation_cache = SampleComputationCache(Dict{UInt,Float64}(), Dict{UInt,Int64}(), Dict{UInt,Any}())

struct SampleComputationCache
    computation::Dict{UInt,Any}
    same_keys::Dict{UInt,Vector{UInt}}
end

global sample_computation_cache = SampleComputationCache(Dict{UInt,Any}(), Dict{UInt,Vector{UInt}}())
# global sample_computation_cache_same_keys = Dict{UInt,Vector{UInt}}()

function get_sample_computation_cache()::SampleComputationCache
    global sample_computation_cache
    sample_computation_cache
end
# function get_sample_computation_cache_same_keys()::Dict{UInt,UInt}
#     global sample_computation_cache_same_keys
#     sample_computation_cache_same_keys
# end
# function get_cached_sample_computation(key::UInt)
#     computation_cache = get_sample_computation_cache()
#     same_key_cache = get_sample_computation_cache()
# end

# function get_futures(samples_for_grouping::SampleForGrouping{T,K})
#     Future[samples_for_grouping.future]
# end
# function get_futures(samples_for_grouping::Vector{SampleForGrouping{T,K}})
#     res = Future[]
#     for s in samples_for_grouping
#         push!(res, s.future)
#     end
#     res
# end

# # Global map from future-key pair to a sample that has the same statistics
# # properties for that particular key
# same_statistics = Dict{Tuple{ValueId,<:Any},Any}()

function insert_in_sample_computation_cache(cache::SampleComputationCache, key::UInt, other_key::UInt)
    if !haskey(cache.same_keys, key)
        cache.same_keys[key] = UInt[key, other_key]
    else
        push!(cache.same_keys[key], other_key)
    end
end

function get_key_for_sample_computation_cache(cache::SampleComputationCache, key::UInt)::UInt
    if !haskey(cache.same_keys, key)
        cache.same_keys[key] = UInt[key]
        return 0
    end

    for other_key in cache.same_keys[key]
        if haskey(cache.computation, other_key)
            return other_key
        end
    end

    return 0
end

# TODO: Finish this

function keep_same_statistics(a::Future, a_key::Any, b::Future, b_key::Any)
    cache = get_sample_computation_cache()
    # Note that this runs after all samples have been computed so the objectid's
    # of the values should be right.
    a_objectid::UInt = get_location(a).sample.objectid
    b_objectid::UInt = get_location(b).sample.objectid
    for computation_func in [:sample_divisions, :orderinghashes]
        a_cache_key = hash((computation_func, a_objectid, a_key))
        b_cache_key = hash((computation_func, b_objectid, b_key))
        insert_in_sample_computation_cache(cache, a_cache_key, b_cache_key)
        insert_in_sample_computation_cache(cache, b_cache_key, a_cache_key)
    end
    # global same_statistics
    # a_map_key = (a.value_id, a_key)
    # b_map_key = (b.value_id, b_key)
    # sample_with_same_statistics = if haskey(same_statistics, a_map_key)
    #     same_statistics[a_map_key]
    # elseif haskey(same_statistics, b_map_key)
    #     same_statistics[b_map_key]
    # else
    #     sample(a)
    # end
    # same_statistics[a_map_key] = sample_with_same_statistics
    # same_statistics[b_map_key] = sample_with_same_statistics
end

# TODO: Look at keep_same_statistics and use the right sample but then use the caching appropriately

# function delete_same_stastics(v::ValueId)
#     global same_statistics
#     for gk in get_location(v).sample.groupingkeys
#         delete!(same_statistics, (v, gk))
#     end
# end