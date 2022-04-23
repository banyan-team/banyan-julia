mutable struct DelayedTask
    # Fields for use in processed task ready to be recorded
    used_modules::Vector{String}
    code::String
    value_names::Vector{Tuple{ValueId,String}}
    effects::Dict{ValueId,String}
    pa_union::Vector{PartitionAnnotation} # Enumeration of applicable PAs
    memory_usage::Dict{ValueId,Dict{String,Int64}} # initial/additional/final
    # Fields for use in task yet to be processed in a call to `compute`
    partitioned_using_func::PartitionedUsingFunc
    partitioned_with_func::Function
    futures::Vector{Future}
    mutation::IdDict{Future,Future} # This gets converted to `effects`
    # Fields for estimating memory usage
    inputs::Vector{Future}
    outputs::Vector{Future}
    scaled::Vector{Future}
    keep_same_sample_rate::Bool
    memory_usage_constraints::Vector{PartitioningConstraint}
    additional_memory_usage_constraints::Vector{PartitioningConstraint}
    # Fields for final serialized task
    input_value_ids::Vector{ValueId}
    output_value_ids::Vector{ValueId}
    scaled_value_ids::Vector{ValueId}
end