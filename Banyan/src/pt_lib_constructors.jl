# NOTE: Do not construct a PT such that PTs can be fused together or used as-is
# in a way such that there aren't functions for splitting and merging them in
# pt_lib.jl. Note that each splitting and merging function in pt_lib.jl is for
# specific locations and so, for example, a Div should not be used on a value
# with CSV location unless there is a splitting function for that.

Block() = PartitionType(Dict("name" => "Block"))
Block(dim) = PartitionType(Dict("name" => "Block", "dim" => dim))
BlockBalanced() = PartitionType(Dict("name" => "Block", "balanced" => true))
BlockBalanced(dim) =
    PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
BlockUnbalanced() = PartitionType(Dict("name" => "Block", "balanced" => false))
BlockUnbalanced(dim) =
    PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => false))
    
Div() = PartitionType(Dict("name" => "Replicate", "dividing" => true))
Replicated() = PartitionType(Dict("name" => "Replicate", "replicated" => true))
Reducing(op) = PartitionType(Dict("name" => "Replicate", "replicated" => false, "reducer" => to_jl_value(op)))

# TODO: Generate AtMost and ScaledBy constraints in handling filters and joins
# that introduce data skew and in other operations that explicitly don't
