mutable struct Sample
    # The sample itself
    value::Any
    objectid::UInt
    # Properties of the sample
    memory_usage::Int64
    rate::Int64
    # TODO: Maybe make grouping key generic K
    groupingkeys::Vector{<:Any}

    Sample() =
        new(nothing, objectid(nothing), 0, get_sample_rate(), Any[])
    # Sample(value::Any) =
    #     new(value, objectid(value), sample_memory_usage(value), get_sample_rate(), Any[])
    function Sample(value::Any, total_memory_usage::Int64, sample_rate::Int64)
        # sample_rate = get_sample_rate()
        memory_usage = convert(Int64, round(total_memory_usage / sample_rate))::Int64
        new(value, objectid(value), memory_usage, sample_rate, Any[])
    end
    function Sample(value::Any, sample_rate::Int64)
        # This is only for the NOTHING_SAMPLE and ExactSample
        new(value, objectid(value), sample_memory_usage(value), sample_rate, Any[])
    end
end

struct SamplingConfig
    rate::Int64
    always_exact::Bool
    max_num_bytes_exact::Int64
    force_new_sample_rate::Bool
    assume_shuffled::Bool
end

const DEFAULT_SAMPLING_CONFIG = SamplingConfig(1024, false, parse_bytes("256 MB"), false, true)
session_sampling_configs = Dict{SessionId,Dict{LocationPath,SamplingConfig}}("" => Dict(NO_LOCATION_PATH => DEFAULT_SAMPLING_CONFIG))