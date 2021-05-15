###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

mutable struct Sample
    # The sample itself
    value::Any
    # Properties of the sample
    properties::Dict{String,Any}

    function Sample(value::Any = nothing, properties::Dict{String,Any} = Dict())
        properties[:rate] = if isnothing(sample) get_job().sample_rate else 1 end
        new(value, properties)
    end
    # TODO: Un-comment if needed
    # Sample(sample::Sample, properties::Vector{String}) =
    #     new(sample.value, sample.stale, Dict(
    #         sample.sample_properties[prop]
    #         for prop in properties
    #     ))
end

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

sample(as::AbstractSampleWithKeys, properties...) =
    if length(properties) == 1
        if first(properties) == :keys
            sample_keys(as)
        elseif first(properties) == :groupingkeys
            sample_groupingkeys(as)
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    elseif length(properties) == 3 && first(properties) == :statistics
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
    elseif length(properties) == 4 && first(properties) == :statistics
        key, query, value = properties[2:end]
        if query == :division
            sample_division(as, key, value)
        else
            throw(ArgumentError("Invalid sample properties: $properties"))
        end
    else
        throw(ArgumentError("Invalid sample properties: $properties"))
    end

# Implementation error 
abstract_sample_with_keys_impl_error(fn_name) =
    error("$fn_name not implemented for $(typeof(as)) <: AbstractSampleWithKeys")
const aswkie = abstract_sample_with_keys_impl_error

# For determining what a value can be grouped by
sample_keys(as::AbstractSampleWithKeys) = aswkie("sample_keys")
sample_groupingkeys(as::AbstractSampleWithKeys) = aswkie("sample_groupingkeys")
sample_divisions(as::AbstractSampleWithKeys, key) = aswkie("sample_divisions")

# For determining constraints on grouping
sample_max_ngroups(as::AbstractSampleWithKeys, key) = aswkie("sample_max_ngroups")
sample_division(as::AbstractSampleWithKeys, key, value) = aswkie("sample_division")
sample_min(as::AbstractSampleWithKeys, key) = aswkie("sample_min")
sample_max(as::AbstractSampleWithKeys, key) = aswkie("sample_max")