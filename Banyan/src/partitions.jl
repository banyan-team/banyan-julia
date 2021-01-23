##################
# PARTITION TYPE #
##################

struct PartitionType
    split_name::String
    merge_name::String
    splitting_parameters::Vector{Any}
    merging_parameters::Vector{Any}
    max_npartitions::Int32
end

function to_jl(pt::PartitionType)
    return Dict(
        "split_name" => pt.split_name,
        "merge_name" => pt.merge_name,
        "splitting_parameters" => pt.splitting_parameters,
        "merging_parameters" => pt.merging_parameters,
        "max_npartitions" => pt.max_npartitions,
    )
end

######################### 
# PARTITION CONSTRAINTS #
#########################

#@enum ConstraintType Co Cross Equal Order Sequential

function to_jl(constraint_type)
    #if constraint_type == Co
    #    return "CO"
    #elseif constraint_type == Cross
    #    return "CROSS"
    #elseif constraint_type == Equal
    #    return "EQUAL"
    #elseif constraint_type == Order
    #    return "ORDER"
    #elseif constraint_type == Sequential
    #    return "SEQUENTIAL"
    #end
    return constraint_type
end

const PartitionTypeReference = Tuple{ValueId,Int32}

struct PartitioningConstraint
    type::String
    args::Vector{PartitionTypeReference}
end

function to_jl(constraint::PartitioningConstraint)
    return Dict(
        "type" => to_jl(constraint.type),
        "args" => args
    )
end

struct PartitioningConstraints
    constraints::Set{PartitioningConstraint}
end

function to_jl(constraints::PartitioningConstraints)
    return Dict(
        "constraints" => [to_jl(constraint) for constraint in constraints.constraints]
    )
end


######################## 
# PARTITION ANNOTATION #
########################
struct Partitions
    pt_stacks::Dict{ValueId, Vector{PartitionType}}
end

function to_jl(p::Partitions)
    return Dict(
        "pt_stacks" => Dict(v => [to_jl(pt) for pt in pts] for (v, pts) in p.pt_stacks)
    )
end

struct PartitionAnnotation
    partitions::Partitions
    constraints::PartitioningConstraints
end

function to_jl(pa::PartitionAnnotation)
    return Dict(
        "partitions" => to_jl(pa.partitions),
        "constraints" =>
        to_jl(pa.constraints)
    )
end
