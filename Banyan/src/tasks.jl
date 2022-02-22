#########
# Tasks #
#########

mutable struct DelayedTask
    # Fields for use in processed task ready to be recorded
    used_modules#::Vector
    code#::String
    value_names#::Vector{Tuple{ValueId,String}}
    effects#::Dict{ValueId,String}
    pa_union#::Vector{PartitionAnnotation} # Enumeration of applicable PAs
    memory_usage#::Dict{ValueId,Dict{String,Integer}} # initial/additional/final
    # Fields for use in task yet to be processed in a call to `compute`
    partitioned_using_func#::Union{Function,Nothing}
    partitioned_with_func#::Union{Function,Nothing}
    mutation#::Dict{Future,Future} # This gets converted to `effects`
    # Fields for estimating memory usage
    inputs#::Vector{Future}
    outputs#::Vector{Future}
    scaled#::Vector{Future}
    keep_same_sample_rate#::Bool
    memory_usage_constraints#::Vector{PartitioningConstraint}
    additional_memory_usage_constraints#::Vector{PartitioningConstraint}
end

DelayedTask() = DelayedTask(
    [],
    "",
    [],
    Dict(),
    [PartitionAnnotation()],
    Dict(),
    nothing,
    nothing,
    Dict(),
    [],
    [],
    [],
    true,
    [],
    []
)

function to_jl(task::DelayedTask)
    return Dict(
        "code" => task.code,
        "value_names" => task.value_names,
        "effects" => task.effects,
        "pa_union" => [to_jl(pa) for pa in task.pa_union],
        "memory_usage" => task.memory_usage,
        "inputs" => [i.value_id for i in task.inputs],
        "outputs" => [o.value_id for o in task.outputs],
        "keep_same_sample_rate" => task.keep_same_sample_rate,
    )
end
