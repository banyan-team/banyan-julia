#########
# Tasks #
#########

struct DelayedTask
    # Fields for use in processed task ready to be recorded
    code::String
    value_names::Dict{ValueId,String}
    effects::Dict{ValueId,String}
    pa_union::Vector{PartitionAnnotation} # Enumeration of applicable PAs
    # Fields for use in task yet to be processed in a call to `compute`
    partitioned_using_func::Union{Function,Nothing}
    partitioned_with_func::Union{Function,Nothing}
    mutation::Dict{Future,Future} # This gets converted to `effects`
end

DelayedTask() = DelayedTask("", Dict(), Dict(), [PartitionAnnotation()], nothing, nothing, Dict())

function to_jl(task::DelayedTask)
    return Dict(
        "code" => task.code,
        "value_names" => task.value_names,
        "effects" => task.effects,
        "pa_union" => [to_jl(pa) for pa in task.pa_union],
    )
end
