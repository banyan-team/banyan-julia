struct Task
    code::String
    value_names::Dict{ValueId, String}
    effects::Dict{ValueId, String}
    pa_union::Set{PartitionAnnotation}
end

function to_jl(task::Task)
	return Dict{Any}(
		"code" => task.code,
		"value_names" => task.value_names,
		"effects" => task.effects,
		"pa_union" => [
			to_jl(pa) for pa in task.pa_union
		]
	)
end