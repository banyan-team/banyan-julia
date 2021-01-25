##################
# PARTITION TYPE #
##################

const PartitionTypeParameters = Vector{Dict}

struct PartitionType
    parameters::PartitionTypeParameters
    max_npartitions::Integer
    min_partition_size::Integer

    function PartitionType(
        parameters::Union{String, Dict, PartitionTypeParameters};
        max_npartitions::Integer = -1,
        min_partition_size::Integer = -1
    )
        new(
            if parameters isa String
                [Dict("name" => parameters)]
            elseif typeof(parameters) <: Dict
                [parameters]
            else
                parameters
            end,
            max_npartitions,
            min_partition_size,
        )
    end
end

function to_jl(pt::PartitionType)
    return Dict(
        "parameters" => pt.parameters,
        "max_npartitions" => pt.max_npartitions,
        "min_partition_size" => pt.min_partition_size,
    )
end

const PartitionTypeComposition = Union{PartitionType,Vector{PartitionType}}

pt_composition_to_jl(pts::PartitionTypeComposition) =
    if pts isa PartitionType
        [to_jl(pts)]
    else
        [to_jl(pt) for pt in pts]
    end

############################
# PARTITIONING CONSTRAINTS #
############################

const PartitionTypeReference = Union{Future, Tuple{Future,Integer}}

pt_ref_to_jl(pt_ref::PartitionTypeReference) =
    if pt_ref isa ValueId
        (pt_ref.value_id, 0)
    else
        (pt_ref[1].value_id, pt_ref[2] - 1)
    end

struct PartitioningConstraint
    type::String
    args::Vector{PartitionTypeReference}
end

function to_jl(constraint::PartitioningConstraint)
    return Dict(
        "type" => constraint.type,
        "args" => [pt_ref_to_jl(arg) for arg in args]
    )
end

# TODO: Support Ordered
Co(args...)         = PartitioningConstraint("CO", collect(args))
Cross(args...)      = PartitioningConstraint("CROSS", collect(args))
Equals(args...)     = PartitioningConstraint("EQUALS", collect(args))
Sequential(args...) = PartitioningConstraint("SEQUENTIAL", collect(args))
Matches(args...)    = PartitioningConstraint("MATCHES", collect(args))

struct PartitioningConstraints
    constraints::Vector{PartitioningConstraint}
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
    pt_stacks::Dict{ValueId, PartitionTypeComposition}
end

function to_jl(p::Partitions)
    return Dict(
        "pt_stacks" => Dict(v => pt_composition_to_jl(pts) for (v, pts) in p.pt_stacks)
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
