struct Task
    code::String
	value_names::Dict{ValueId, String}
	locations::Dict{ValueId, LocationType}
    effects::Dict{ValueId, String}
    pa_union::Set{PartitionAnnotation}
end

function to_jl(task::Task)
	return Dict{String, Any}(
		"code" => task.code,
		"value_names" => task.value_names,
		"locations" => Dict(id => to_jl(lt) for (id, lt) in task.locations),
		"effects" => task.effects,
		"pa_union" => [
			to_jl(pa) for pa in task.pa_union
		]
	)
end
