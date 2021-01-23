function Cross(args...)
    args = collect(args)
    for i in 1::size(args, 1)
        if !(typeof(args[i]) <: Tuple)
            args[i] = (args[i], 0)
        end
    end
    return PartitioningConstraint(
        "Cross",
        args
    )
end
