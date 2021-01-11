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

function Block(dim = 1)
    pt("Block", [dim], -1)
end