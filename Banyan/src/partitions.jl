##################
# PARTITION TYPE #
##################

const PartitionTypeParameters = Vector{Dict}

struct PartitionType
    parameters::PartitionTypeParameters

    function PartitionType(
        parameters::Union{String,Dict,PartitionTypeParameters},
    )
        new(if parameters isa String
            [Dict("name" => parameters)]
        elseif typeof(parameters) <: Dict
            [parameters]
        else
            parameters
        end)
    end
end

function Base.getproperty(pt::PartitionType, name::Symbol)
    if hasfield(PartitionType, name)
        return getfield(pt, name)
    end

    n = string(name)
    for parameters in pt.parameters
        if haskey(parameters, n)
            return parameters[n]
        end
    end
    error("$name not found in location parameters")
end

function to_jl(pt::PartitionType)
    return Dict("parameters" => pt.parameters)
end

const PartitionTypeComposition = Union{PartitionType,Vector{PartitionType}}

pt_composition_from_pts(
    pt_composition::PartitionTypeComposition,
)::Vector{PartitionType} =
    if pt_composition isa Vector
        pt_composition
    else
        [pt_composition]
    end

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

struct PartitioningConstraintOverGroups
    type::String
    args::Vector{Vector{PartitionTypeReference}}
end

function to_jl(
    constraint::Union{PartitioningConstraint,PartitioningConstraintOverGroups},
)
    return Dict("type" => constraint.type, "args" => constraint.args)
end

arg_to_jl_for_co(arg) =
    if arg isa Vector
        pt_refs_to_jl(arg)
    else
        [pt_ref_to_jl(arg)]
    end

function constraint_for_co(args)::PartitioningConstraintOverGroups
    if any(arg isa Vector for arg in args)
        args = [arg_to_jl_for_co(arg) for arg in args]
        PartitioningConstraintOverGroups("CO_GROUP", args)
    else
        PartitioningConstraintOverGroups("CO", pt_refs_to_jl(args))
    end
end

# TODO: Support Ordered
Co(args...) = constraint_for_co(args)
Cross(args...) = PartitioningConstraint("CROSS", pt_refs_to_jl(args))
Equal(args...) = PartitioningConstraint("EQUAL", pt_refs_to_jl(args))
Sequential(args...) = PartitioningConstraint("SEQUENTIAL", pt_refs_to_jl(args))
Match(args...) = PartitioningConstraint("MATCH", pt_refs_to_jl(args))
AtMost(npartitions, args...) =
    PartitioningConstraint("AT_MOST=$npartitions", pt_refs_to_jl(args))

struct PartitioningConstraints
    constraints::Vector{
        Union{PartitioningConstraint,PartitioningConstraintOverGroups},
    }
end

function to_jl(constraints::PartitioningConstraints)
    return Dict(
        "constraints" =>
            [to_jl(constraint) for constraint in constraints.constraints],
    )
end

######################## 
# PARTITION ANNOTATION #
########################

struct Partitions
    pt_stacks::Dict{ValueId,PartitionTypeComposition}
end

function to_jl(p::Partitions)
    return Dict(
        "pt_stacks" =>
            Dict(v => pt_composition_to_jl(pts) for (v, pts) in p.pt_stacks),
    )
end

struct PartitionAnnotation
    partitions::Partitions
    constraints::PartitioningConstraints
end

function to_jl(pa::PartitionAnnotation)
    return Dict(
        "partitions" => to_jl(pa.partitions),
        "constraints" => to_jl(pa.constraints),
    )
end
