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

const PartitionTypeReference = Tuple{ValueId,Integer}

pt_ref_to_jl(pt_ref) =
    if pt_ref isa Tuple
        (future(pt_ref[1]).value_id, pt_ref[2] - 1)
    else
        (future(pt_ref).value_id, 0)
    end

pt_refs_to_jl(refs) = [pt_ref_to_jl(ref) for ref in refs]

struct PartitioningConstraint
    type::String
    args::Vector{PartitionTypeReference}
end

function to_jl(constraint::PartitioningConstraint)
    return Dict(
        "type" => constraint.type,
        "args" => constraint.args
    )
end

# TODO: Support Ordered
Co(args...)         = PartitioningConstraint("CO", pt_refs_to_jl(args))
Cross(args...)      = PartitioningConstraint("CROSS", pt_refs_to_jl(args))
Equal(args...)      = PartitioningConstraint("EQUAL", pt_refs_to_jl(args))
Sequential(args...) = PartitioningConstraint("SEQUENTIAL", pt_refs_to_jl(args))
Match(args...)      = PartitioningConstraint("MATCH", pt_refs_to_jl(args))
MaxNPartitions(npartitions, args...) =
    PartitioningConstraint("MAX_NPARTITIONS=$npartitions", pt_refs_to_jl(args))
MinPartitionSize(size, args...) =
    PartitioningConstraint("MIN_PARTITION_SIZE=$size", pt_refs_to_jl(args))

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
