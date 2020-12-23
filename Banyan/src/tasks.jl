struct Task
    code::String
    value_names::Dict{ValueId, String}
    pa_union::Set{PartitionAnnotation}
end