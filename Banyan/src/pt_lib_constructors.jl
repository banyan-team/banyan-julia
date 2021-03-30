# NOTE: Do not construct a PT such that PTs can be fused together or used as-is
# in a way such that there aren't functions for splitting and merging them in
# pt_lib.jl. Note that each splitting and merging function in pt_lib.jl is for
# specific locations and so, for example, a Div should not be used on a value
# with CSV location unless there is a splitting function for that.

Block() = PartitionType(Dict("name" => "Block"))
Block(dim) = PartitionType(Dict("name" => "Block", "dim" => dim))
# BlockBalanced() = PartitionType(Dict("name" => "Block", "balanced" => true))
# BlockBalanced(dim) =
#     PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
# Div() = PartitionType(Dict("name" => "Replicate", "dividing" => true))
# Replicate() = PartitionType(Dict("name" => "Replicate"))
