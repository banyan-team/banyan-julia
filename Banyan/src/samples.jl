function configure_sampling(
    path="";
    sample_rate=nothing,
    always_exact=nothing,
    max_num_bytes_exact=nothing,
    force_new_sample_rate=nothing,
    assume_shuffled=nothing,
    for_all_locations=false,
    default=false,
    kwargs...
)
    global session_sampling_configs

    sc = get_sampling_config(path; kwargs...)
    nsc = SamplingConfig(
        (!isnothing(sample_rate) && !default) ? rate : sc.rate,
        (!isnothing(always_exact) && !default) ? always_exact : sc.always_exact,
        (!isnothing(max_num_bytes_exact) && !default) ? max_num_bytes_exact : sc.max_num_bytes_exact,
        (!isnothing(force_new_sample_rate) && !default) ? force_new_sample_rate : sc.force_new_sample_rate,
        (!isnothing(assume_shuffled) && !default) ? assume_shuffled : sc.assume_shuffled,
    )

    session_id = _get_session_id_no_error()
    lp = get_location_path_with_format(path; kwargs...)
    sampling_configs = session_sampling_configs[session_id]
    if for_all_locations
        empty!(sampling_configs)
        sampling_configs[NO_LOCATION_PATH] = nsc
    else
        sampling_configs[lp] = nsc
    end
    
end

###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

ExactSample(value::Any) = Sample(value, 1)
ExactSample(value::Any, memory_usage::Int64) = Sample(value, memory_usage, 1)

function setsample!(fut::Future, value::Any)
    s::Sample = get_location(fut).sample
    memory_usage::Int64 = sample_memory_usage(value)
    rate::Int64 = s.rate
    s.value = value
    s.memory_usage = memory_usage
    if Banyan.INVESTIGATING_MEMORY_USAGE
        println("In setsample!")
        @show memory_usage rate s.memory_usage fut.value_id
    end
    s.objectid = objectid(value)
end

# sample* functions always return a concrete value or a dict with properties.
# To get a `Sample`, access the property.

# TODO: Lazily compute samples by storing sample computation in a DAG if its
# getting too expensive
sample(fut::AbstractFuture) = get_location(convert(Future, fut)::Future).sample.value
sample(fut::Future) = get_location(fut).sample.value

# Implementation error 
impl_error(fn_name, as) = error("$fn_name not implemented for $(typeof(as))")

# Functions to implement for Any (e.g., for DataFrame or Array

sample_by_key(as::Any, key::Any) = impl_error("sample_by_key", as)
sample_axes(as::Any)::Vector{Int64} = impl_error("sample_axes", as)
sample_keys(as::Any) = impl_error("sample_keys", as)

# Sample computation functions

function orderinghashes(df::T, key::K) where {T,K}
    cache = get_sample_computation_cache()
    cache_key = hash((:orderinghashes, objectid(df), key))
    in_cache = get_key_for_sample_computation_cache(cache, cache_key)
    if in_cache != 0
        return cache.computation[in_cache]
    end
    data = sample_by_key(df, key)
    res = map(orderinghash, data)
    cache.computation[cache_key] = res
    res
end

function get_all_divisions(data::Base.Vector{OHT}, ngroups::Int64)::Base.Vector{Tuple{OHT,OHT}} where {OHT}
    datalength = length(data)
    grouplength = div(datalength, ngroups)
    # We use `unique` here because if the divisions have duplicates, this could
    # result in different partitions getting the same divisions. The usage of
    # `unique` here is more of a safety precaution. The # of divisions we use
    # is the maximum # of groups.
    # TODO: Ensure that `unique` doesn't change the order
    all_divisions::Base.Vector{Tuple{OHT,OHT}} = Tuple{OHT,OHT}[]
    used_index_pairs::Base.Vector{Tuple{Int64,Int64}} = Tuple{Int64,Int64}[]
    for i = 1:ngroups
        startindex = (i-1)*grouplength + 1
        endindex = i == ngroups ? datalength : i*grouplength + 1
        index_pair = (startindex, endindex)
        if !(index_pair in used_index_pairs)
            push!(
                all_divisions,
                # Each group has elements that are >= start and < end
                (
                    data[startindex],
                    data[endindex]
                )
            )
            push!(used_index_pairs, index_pair)
        end
    end
    all_divisions
end

function sample_divisions(df::T, key::K) where {T,K}
    cache = get_sample_computation_cache()
    cache_key = hash((:sample_divisions, objectid(df), key))
    in_cache = get_key_for_sample_computation_cache(cache, cache_key)
    if in_cache != 0
        return cache.computation[in_cache]
    end

    # There are no divisions for empty data
    if isempty(df)
        return Any[]
    end

    max_ngroups = sample_max_ngroups(df, key)
    ngroups = min(max_ngroups, 512)
    data_unsorted = orderinghashes(df, key)
    data = sort(data_unsorted, lt=(<=))
    all_divisions = get_all_divisions(data, ngroups)
    cache.computation[cache_key] = all_divisions
    all_divisions
end

function sample_percentile(df::T, key::K, minvalue, maxvalue)::Float64 where {T,K}
    # If the data frame is empty, nothing between `minvalue` and `maxvalue` can
    # exist in `df`. so the percentile is 0.
    if isempty(df) || isnothing(minvalue) || isnothing(maxvalue)
        return 0.0
    end

    # NOTE: This may cause some problems because the way that data is ultimately split may
    # not allow a really fine division of groups. So in the case of some filtering, the rate
    # of filtering may be 90% but if there are only like 3 groups then maybe it ends up being like
    # 50% and that doesn't get scheduled properly. We can try to catch stuff like maybe by using
    # only 70% of total memory in scheduling or more pointedly by changing this function to
    # call sample_divisions with a reasonable number of divisions and then counting how many
    # divisions the range actually belongs to.

    c::Int64 = 0
    num_rows::Int64 = 0
    ohs = orderinghashes(df, key)
    for oh in ohs
        if minvalue <= oh && oh <= maxvalue
            c += 1
        end
        num_rows += 1
    end
    c / num_rows
end

function sample_max_ngroups(as::T, key::K)::Int64 where {T,K}
    if isempty(as)
        0
    else
        data = sample_by_key(as, key)
        data_counter = counter(data)
        max_nrow_group = maximum(values(data_counter))
        div(length(data), max_nrow_group)
    end
end

function _minimum(ohs::Vector{OHT})::OHT where {OHT}
    oh_min = ohs[1]
    for oh in ohs
        oh_min = oh <= oh_min ? oh : oh_min
    end
    oh_min
end

function _maximum(ohs::Vector{OHT})::OHT where {OHT}
    oh_max = ohs[1]
    for oh in ohs
        oh_max = oh_max <= oh ? oh : oh_max
    end
    oh_max
end

# TODO: Maybe instead just do `orderinghash(minimum(sample_by_key(A, key)))``

function sample_min(A::T, key::K) where {T,K}
    isempty(A) ? nothing : _minimum(orderinghashes(A, key))
end

function sample_max(A::T, key::K) where {T,K}
    isempty(A) ? nothing : _maximum(orderinghashes(A, key))
end

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
function sample_for_grouping(f::Future, ::Type{K}) where {K} sample_for_grouping(f, convert(Vector{K}, get_location(f).sample.groupingkeys)::Vector{K}) end
sample_for_grouping(f::Future) = sample_for_grouping(f, Int64)

struct SampleComputationCache
    computation::Dict{UInt,Any}
    same_keys::Dict{UInt,Vector{UInt}}
end

global sample_computation_cache = SampleComputationCache(Dict{UInt,Any}(), Dict{UInt,Vector{UInt}}())

function get_sample_computation_cache()::SampleComputationCache
    global sample_computation_cache
    sample_computation_cache
end

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
end
