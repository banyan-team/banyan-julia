Block(dim = nothing) =
    if isnothing(dim)
        PartitionType(Dict("name" => "Block"))
    else
        PartitionType(Dict("name" => "Block", "dim" => dim))
    end