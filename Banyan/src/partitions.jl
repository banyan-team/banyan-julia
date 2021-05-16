###################
# Partition types #
###################

const PartitionTypeParameters = Dict{String, Any}

mutable struct PartitionType
    parameters::PartitionTypeParameters
    constraints::PartitioningConstraints

    PartitionType(s::String) = new(Dict("name" => s), PartitioningConstraints())
    PartitionType(parameters::PartitionTypeParameters) = new(parameters, PartitioningConstraints())

    function PartitionType(args::Union{String, Pair{String,Any}, PartitioningConstraint}...)
        parameters = Dict()
        constraints = PartitioningConstraints()

        # Construct parameters and constraints from arguments
        for arg in args
            if arg isa String
                parameters["name"] = arg
            elseif arg isa Pair
                parameters[first(arg)] = last(arg)
            elseif arg isa PartitioningConstraint
                push!(constraints, arg)
            else
                throw(ArgumentError("Expected either a partition type parameter or constraint"))
            end
        end

        new(parameters, constraints)
    end
end

# TODO: Determine whether we need this
# function Base.getproperty(pt::PartitionType, name::Symbol)
#     if hasfield(PartitionType, name)
#         return getfield(pt, name)
#     end

#     n = string(name)
#     if haskey(pt.parameters, n)
#         return pt.parameters[n]
#     end
#     error("$name not found in partition type parameters")
# end

function to_jl(pt::PartitionType)
    # Interpret bangs as random IDs
    # TODO: Use this in the backend to interpret pt_lib_info.json
    for (k, v) in pt.parameters
        if v == "!"
            pt.parameters[k] = randstring(8)
        end
    end

    # Construct dictionary
    Dict("parameters" => pt.parameters, "constraints" => to_jl(pt.constraints))
end

struct PTComposition
    pts::Vector{PartitionType}
end

struct PTUnion
    pts::Vector{PartitionType}
end

to_jl(ptc::PTComposition) = [to_jl(pt) for pt in ptc.pts]

const PartitionTypeReference = Tuple{ValueId,Integer}

############################
# Partitioning constraints #
############################

pt_ref_to_jl(pt_ref) =
    if pt_ref isa Tuple
        (convert(Future, pt_ref[1]).value_id, pt_ref[2] - 1)
    else
        (convert(Future, pt_ref).value_id, 0)
    end

pt_refs_to_jl(refs::Vector{PartitionTypeReference}) =
    [pt_ref_to_jl(ref) for ref in refs]

struct PartitioningConstraintOverGroup
    type::String
    args::Vector{PartitionTypeReference}
end

struct PartitioningConstraintOverGroups
    type::String
    args::Vector{Vector{PartitionTypeReference}}
end

const PartitioningConstraint = Union{PartitioningConstraintOverGroup, PartitioningConstraintOverGroups}

to_jl(pc::PartitioningConstraint) = Dict("type" => pc.type, "args" => pc.args)

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
Cross(args...) = PartitioningConstraintOverGroup("CROSS", pt_refs_to_jl(args))
Equal(args...) = PartitioningConstraintOverGroup("EQUAL", pt_refs_to_jl(args))
Sequential(args...) =
    PartitioningConstraintOverGroup("SEQUENTIAL", pt_refs_to_jl(args))
Match(args...) = PartitioningConstraintOverGroup("MATCH", pt_refs_to_jl(args))
MatchOn(args...) =
    PartitioningConstraintOverGroup(
        "MATCH_ON=" * string(args[end]),
        pt_refs_to_jl(args[1:end-1]),
    )
AtMost(npartitions, args...) =
    PartitioningConstraintOverGroup(
        "AT_MOST=$npartitions",
        pt_refs_to_jl(args)
    )
ScaledBy(factor::Float32 = 1.0, args...) = 
    PartitioningConstraintOverGroup(
        "SCALED_BY=$factor",
        pt_refs_to_jl(args)
    )
# TODO: Fuse ScaledBy constraints in scheduler to be dependent only on factor

mutable struct PartitioningConstraints
    constraints::Vector{PartitioningConstraint}
end

PartitioningConstraints() = PartitioningConstraints([])

function to_jl(constraints::PartitioningConstraints)
    return Dict(
        "constraints" =>
            [to_jl(constraint) for constraint in constraints.constraints],
    )
end

#########################
# Partition annotations #
#########################

# TODO: Rename Partitions to PartitionTypeBinding and keep Partitioning as is
mutable struct Partitions
    # TODO: Only use either PT stack or PT composition to be consistent in
    # terminology
    pt_stacks::Dict{ValueId,PTComposition}
end

Partitions() = Partitions(Dict())

function to_jl(p::Partitions)
    # NOTE: This assumes that the PT compositions in `p.pt_stacks` are _not_
    # delayed
    return Dict(
        "pt_stacks" =>
            Dict(v => ptc |> pt_composition_to_jl for (v, ptc) in p.pt_stacks),
    )
end

mutable struct PartitionAnnotation
    partitions::Partitions
    constraints::PartitioningConstraints
end

PartitionAnnotation() = PartitionAnnotation(Partitions(), PartitioningConstraints())

function to_jl(pa::PartitionAnnotation)
    return Dict(
        "partitions" => to_jl(pa.partitions),
        "constraints" => to_jl(pa.constraints),
    )
end
