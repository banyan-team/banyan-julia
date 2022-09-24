#########
# Tasks #
#########

DelayedTask() = DelayedTask(
    String[],
    "",
    Tuple{ValueId,String}[],
    Dict{ValueId,String}(),
    PartitionAnnotation[PartitionAnnotation()],
    Dict{ValueId,Dict{String,Int64}}(),
    deepcopy(NOTHING_PARTITIONED_USING_FUNC),
    identity,
    Future[],
    IdDict{Future,Future}(),
    Future[],
    Future[],
    Future[],
    true,
    PartitioningConstraint[],
    PartitioningConstraint[],
    ValueId[],
    ValueId[],
    ValueId[]
)

function to_jl(task::DelayedTask)
    return Dict{String,Any}(
        "code" => task.code,
        "value_names" => task.value_names,
        "effects" => task.effects,
        "pa_union" => [to_jl(pa) for pa in task.pa_union],
        "memory_usage" => task.memory_usage,
        "inputs" => task.input_value_ids,
        "outputs" => task.output_value_ids,
        "keep_same_sample_rate" => task.keep_same_sample_rate,
    )
end
