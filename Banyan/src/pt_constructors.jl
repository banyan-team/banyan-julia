function pt(name, parameters, max_npartitions)
    pt = PartitionType(
        name,
        name,
        parameters,
        parameters,
        max_npartitions
    )
    return pt
end

# Value

function Value(fut::Future)
    pt("Value", [fut.value, sizeof(fut.value)], -1)
end

# Block

function Block(dim = 1)
    pt("Block", [dim], -1)
end