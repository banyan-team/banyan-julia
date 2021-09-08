###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

mutable struct Sample
    # The sample itself
    value::Any
    # Properties of the sample
    properties::Dict{Symbol,Any}

    function Sample(
        value::Any = nothing;
        properties::Dict{Symbol,Any} = Dict{Symbol,Any}(),
        sample_rate=get_job().sample_rate,
        total_memory_usage=nothing
    )
        newsample = new(value, properties)

        # We compute the `memory_usage` lazily

        # Fill in properties if possible
        if !isnothing(total_memory_usage)
            setsample!(newsample, :memory_usage, round(total_memory_usage / sample_rate))
        end
        setsample!(newsample, :rate, sample_rate)

        newsample
    end
    # TODO: Un-comment if needed
    # Sample(sample::Sample, properties::Vector{String}) =
    #     new(sample.value, sample.stale, Dict(
    #         sample.sample_properties[prop]
    #         for prop in properties
    #     ))
end

ExactSample(value::Any = nothing; kwargs...) = Sample(value; sample_rate=1, kwargs...)

# TODO: Lazily compute samples by storing sample computation in a DAG if its
# getting too expensive
sample(fut::AbstractFuture) = sample(get_location(fut).sample)
sample(sample::Sample) = sample.value

sample(fut::AbstractFuture, propertykeys...) = sample(get_location(fut).sample, propertykeys...)
function sample(s::Sample, propertykeys...)
    properties = s.properties
    for (i, propertykey) in enumerate(propertykeys)
        properties = get!(
            properties,
            propertykey,
            if i < length(propertykeys)
                Dict()
            else
                sample(s.value, propertykeys...)
            end
        )
    end
    properties
end

setsample!(fut::AbstractFuture, value) = setsample!(get_location(fut).sample, value)
function setsample!(sample::Sample, value)
    sample.value = value
end

setsample!(fut::AbstractFuture, propertykeys...) = setsample!(get_location(fut).sample, propertykeys...)
function setsample!(sample::Sample, propertykeys...)
    if length(propertykeys) == 1
        setsample!(sample, first(propertykeys))
    else
        properties = sample.properties
        propertyvalue = last(propertykeys)
        propertykeys = propertykeys[1:end-1]
        for (i, propertykey) in enumerate(propertykeys)
            if i < length(propertykeys)
                properties = get!(properties, propertykey, Dict())
            end
        end
        properties[last(propertykeys)] = propertyvalue
    end
end

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

sample(as::Any, properties...) =
    if length(properties) <= 2 && first(properties) == :statistics
        # If the statistic is not cached in `Sample.properties`, then we just
        # return an empty dictionary
        Dict()
    elseif length(properties) == 1
        if first(properties) == :memory_usage
            sample_memory_usage(as)
        elseif first(properties) == :rate
            # This is the default but the `Sample` constructor overrides this
            # before-hand to allow some samples to be "exact" with a sample
            # rate of 1
            get_job().sample_rate
        elseif first(properties) == :keys
            sample_keys(as)
        elseif first(properties) == :axes
            sample_axes(as)
        elseif first(properties) == :groupingkeys
            # This is just the initial value for grouping keys. Calls to
            # `keep_*` functions will expand it.
            []
        else
            # println(typeof(as))
            # println(typeof(as) <: Any)
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    elseif length(properties) == 3 && first(properties) == :statistics
        key = properties[2]
        query = properties[3]
        if query == :max_ngroups
            sample_max_ngroups(as, key)
        elseif query == :divisions
            sample_divisions(as, key)
        elseif query == :min
            sample_min(as, key)
        elseif query == :max
            sample_max(as, key)
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    elseif length(properties) == 5 && first(properties) == :statistics
        key, query, minvalue, maxvalue = properties[2:end]
        if query == :percentile
            sample_percentile(as, key, minvalue, maxvalue)
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    else
        throw(ArgumentError("Invalid sample properties: $properties"))
    end

sample_memory_usage(as::Any) = total_memory_usage(as)

# Implementation error 
impl_error(fn_name, as) = error("$fn_name not implemented for $(typeof(as))")

# Functions to implement for Any (e.g., for DataFrame or
# Array)
sample_axes(as::Any) = impl_error("sample_axes", as)
sample_keys(as::Any) = impl_error("sample_keys", as)
sample_divisions(as::Any, key) = impl_error("sample_divisions", as)
sample_percentile(as::Any, key, minvalue, maxvalue) = impl_error("sample_percentile", as)
sample_max_ngroups(as::Any, key) = impl_error("sample_max_ngroups", as)
sample_min(as::Any, key) = impl_error("sample_min", as)
sample_max(as::Any, key) = impl_error("sample_max", as)