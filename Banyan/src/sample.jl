mutable struct Sample
    # The sample itself
    value::Any
    # Properties of the sample
    memory_usage::Int64
    rate::Int64
    # TODO: Maybe make grouping key generic K
    groupingkeys::Vector{<:Any}

    Sample() =
        new(nothing, 0, get_session().sample_rate, Any[])
    Sample(value::Any) =
        new(value, sample_memory_usage(value), get_session().sample_rate, Any[])
    function Sample(value::Any, memory_usage::Int64)
        sample_rate = get_session().sample_rate
        memory_usage = convert(Int64, round(memory_usage / sample_rate))::Int64
        new(value, memory_usage, sample_rate, Any[])
    end
    function Sample(value::Any, memory_usage::Int64, rate::Int64)
        # This is only for the NOTHING_SAMPLE and ExactSample
        new(value, memory_usage, rate, Any[])
    end
end
