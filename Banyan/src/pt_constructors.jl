function pt(name, parameters)
    pt = PartitionType(
        name,
        name,
        parameters,
        parameters
    )
    return pt
end

function Div(fut)
    return pt("Div", [fut.value], -1)
end

function Block(dim = 1)
    return pt("Block", [dim], -1)
end

function Stencil(dim, left_overlap, right_overlap)
    return pt("Stencil", [dim, left_overlap, right_overlap], -1)
end