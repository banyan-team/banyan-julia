###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

mutable struct Sample
    # The sample itself
    value::Any
    # Properties of the sample
    properties::Dict{String,Any}

    function Sample(value::Any, properties::Dict{String,Any})
        properties[:rate] = get_job().sample_rate
        new(value, properties)
    end
    # TODO: Un-comment if needed
    # Sample(sample::Sample, properties::Vector{String}) =
    #     new(sample.value, sample.stale, Dict(
    #         sample.sample_properties[prop]
    #         for prop in properties
    #     ))
end

Sample() = Sample(nothing, Dict())
Sample(value::Any) = Sample(value, Dict())

# TODO: Lazily compute samples by storing sample computation in a DAG if its
# getting too expensive
sample(fut) = get_location(fut).sample.value

function sample(fut, propertykeys...)
    properties = get_location(fut).sample.properties
    for (i, propertykey) in enumerate(propertykeys)
        properties = get!(
            properties,
            propertykey,
            if i < length(propertykeys) Dict() else nothing end
        )
    end
    properties
end

function setsample!(fut, value)
    get_location(fut).sample.value = value
end

function setsample!(fut, propertykeys...)
    if length(propertykeys) == 1
        setsample!(fut, first(propertykeys))
    else
        properties = get_location(fut).sample.properties
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

abstract type AbstractSample end

# The purpose of the `sample` function is to allow for computing various
# properties of the sample by property key name instead of an explicit
# function call. This makes it easier for the `sample` and `setsample!`
# functions for `Future`s to compute and cache samples.

sample(as::AbstractSample, properties...) =
    if length(properties) == 1
        if first(properties) == :memory_usage
        sample_memory_usage(as)
        elseif first(properties) == :memory_usage
            get_job().sample_rate
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    else
        throw(ArgumentError("Invalid sample properties: $properties"))
    end

sample_memory_usage(as::AbstractSample) = total_memory_usage(as)
# TODO: Include sample_rate and have functions for setting it in forward pass

abstract type AbstractSampleWithKeys <: AbstractSample end

# TODO: Implement this for dataframe and for array

sample(as::AbstractSample, properties...) =
    if length(properties) == 1
        if first(properties) == :keys
            sample_keys(as)
        elseif first(properties) == :groupingkeys
            sample_groupingkeys(as)
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    elseif length(properties) == 2
        if first(properties) == :max_ngroups
            sample_max_ngroups(as, last(properties))
        elseif first(properties) == :divisions
            sample_divisions(as, last(properties))
        elseif first(properties) == :min
            sample_min(as, last(properties))
        elseif first(properties) == :max
            sample_max(as, last(properties))
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    elseif length(properties) == 3 && first(properties) == :sample_division
        sample_division(as, properties[2:end]...)
    else
        throw(ArgumentError("Invalid sample properties: $properties"))
    end

abstract_sample_impl_error(fn_name) =
    error("$fn_name not implemented for $(typeof(as)) <: AbstractSampleWithKeys")

sample_keys(as::AbstractSampleWithKeys) = abstract_sample_impl_error("sample_keys")
sample_groupingkeys(as::AbstractSampleWithKeys) =
    abstract_sample_impl_error("sample_groupingkeys")

sample_max_ngroups(as::AbstractSampleWithKeys, key) =
    abstract_sample_impl_error("sample_max_ngroups")
sample_divisions(as::AbstractSampleWithKeys, key) =
    abstract_sample_impl_error("sample_divisions")

sample_division(as::AbstractSampleWithKeys, key, value) =
    abstract_sample_impl_error("sample_division") # quantile of `key` of `as` at `value``
sample_min(as::AbstractSampleWithKeys, key) = abstract_sample_impl_error("sample_min")
sample_max(as::AbstractSampleWithKeys, key) = abstract_sample_impl_error("sample_max")