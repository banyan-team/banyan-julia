function pa_noconstraints(pt_stacks::Dict{ValueId, Vector{PartitionType}})
    pa = PartitionAnnotation(
        Partitions(pt_stacks),
        PartitioningConstraints(Set())
    )
    return pa
end