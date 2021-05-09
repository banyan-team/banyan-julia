mutable struct Sample
    # The sample itself
    value::Any
    # Whether or not the sample is stale and needs to be computed first
    stale::Bool
    # Properties of the sample. Any value in `sample_properties` must not be
    # stale.
    sample_properties::Dict{String,Any}

    
    Sample() = new(nothing, false, Dict())
    Sample(sample::Sample) =
        new(sample.value, sample.stale, sample.sample_properties)
    Sample(sample::Sample, properties::Vector{String}) =
        new(sample.value, sample.stale, Dict(
            sample.sample_properties[prop]
            for prop in properties
        ))
    Sample(value::Any) =
        new(value, false, Dict())
end

record_sample_computation(f) = push!(get_job().sample_computation, f)

get_sample(fut) = get_location(fut).sample.value

function set_sample(fut, value)
    sample = get_location(fut).sample
    sample.value = value
    sample.stale = false
end

function compute_sample(fut)
    sample = get_location(fut).sample
    # TODO: Store sample computation in a DAG instead of a queue so that only
    # the relevant parts are computed
    if sample.stale
        for c in get_job().sample_computation
            c()
        end
        sample.stale = false
    end
    sample.value
end