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

@nospecialize

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
sample_memory_usage(as::Any) = total_memory_usage(as)
sample_axes(as::Any) = impl_error("sample_axes", as)
sample_keys(as::Any) = impl_error("sample_keys", as)
sample_divisions(as::Any, key) = impl_error("sample_divisions", as)
sample_percentile(as::Any, key, minvalue, maxvalue) = impl_error("sample_percentile", as)
sample_max_ngroups(as::Any, key) = impl_error("sample_max_ngroups", as)
sample_min(as::Any, key) = impl_error("sample_min", as)
sample_max(as::Any, key) = impl_error("sample_max", as)

const NOTHING_SAMPLE = Sample(nothing, -1, -1)

Base.isnothing(s::Sample) = s.rate == -1

# Caching samples with same statistics

# Global map from future-key pair to a sample that has the same statistics
# properties for that particular key
same_statistics = Dict{Tuple{ValueId,<:Any},Any}()

function keep_same_statistics(a::Future, a_key::Any, b::Future, b_key::Any)
    global same_statistics
    a_map_key = (a.value_id, a_key)
    b_map_key = (b.value_id, b_key)
    sample_with_same_statistics = if haskey(same_statistics, a_map_key)
        same_statistics[a_map_key]
    elseif haskey(same_statistics, b_map_key)
        same_statistics[b_map_key]
    else
        sample(a)
    end
    same_statistics[a_map_key] = sample_with_same_statistics
    same_statistics[b_map_key] = sample_with_same_statistics
end

function delete_same_stastics(v::ValueId)
    global same_statistics
    for gk in get_location(v).sample.groupingkeys
        delete!(same_statistics, (v, gk))
    end
end

# This functions is for retrieving a sample of a future with same
# statistics properties with this key. Note that this function is not
# type-stable and its return type isn't knowable so it _will_ result
# in type inference. The best way to deal with that is to make sure to pass
# the result of calling this (or even the other `sample` functions) into a
# separate function to actually process its statistics. This creates a function
# barrier and type inference will still happen at run time but it will only
# happen once.
function sample(f::Future, key::K) where {K}
    global same_statistics
    f_map_key = (f.value_id, key)
    if haskey(same_statistics, f_map_key)
        same_statistics[f_map_key]
    else
        sample(f)
    end
end