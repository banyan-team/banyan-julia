Block(dim = nothing) =
    if isnothing(dim)
        
    else
        PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
    end
Block() = PartitionType(Dict("name" => "Block"))
Block(dim) = PartitionType(Dict("name" => "Block", "dim" => dim))
BlockBalanced() = PartitionType(Dict("name" => "Block", "balanced" => true))
BlockBalanced(dim) = PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
Div() = PartitionType("Div")
Replicate() = PartitionType("Replicate")