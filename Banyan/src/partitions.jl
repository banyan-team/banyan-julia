#############################
# Partition type references #
#############################

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

pt_refs_to_jl(refs) =
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
MatchOn(on, args...) =
    PartitioningConstraintOverGroup(
        "MATCH_ON=" * string(on),
        pt_refs_to_jl(args),
    )
AtMost(npartitions, args...) =
    PartitioningConstraintOverGroup(
        "AT_MOST=$npartitions",
        pt_refs_to_jl(args)
    )
ScaleBy(arg, factor::Real = 1.0, relative_to...) = 
    PartitioningConstraintOverGroup(
        "SCALE_BY=$factor",
        pt_refs_to_jl([arg; relative_to...])
    )

# Co, Cross, Equal, Sequential, Match, MatchOn, AtMost are PA-level constraints.
# AtMost, ScaleBy are PT-level constraints.
# For all constraints, contradictions result in the PA not being used. The
# exception is ScaleBy constraints where multiple PAs can specify a ScaleBy
# for the same PT and these are not fused in any way.

# TODO: Make the above constraint constructors produce dictionaries that have
# fields that make sense and are specialized for each one. This will reduce
# a significant amount of messy and hard-to-read code here (e.g., what in the
# world isa PartitioningConstraintOverGroup vs. a
# PartitioningConstraintOverGroups).

# NOTE: ScaleBy constraints accept PT references but only the values of PT
# references where the index is 1 are taken because we only scale relative
# to the memory usage that is split by the first PT in the PT compositions
# referenced

# NOTE: If you require a constraint for a particular PT, the onus is on you to
# ensure that whereever you use a value with that PT assigned, you always
# have the PTs that are referenced by the constraint. For example, if you use
# an AtMost constraint which references both PTs from a PT composition for a
# value where the first PT splits across workers and the second across batches,
# you need to ensure that anywhere you use the value, you actually do have a 
# PT composition of length 2.

# NOTE: Currently, only AtMost and ScaleBy are supported as PT-level
# constraints (meaning they are included as part of a PT so that the PT
# cannot be applied to a variable unless the constraints are also be enforced)
# while ScaleBy may not be used as PA-level constraints (constraints that are
# applicable only for a single code region annotated with a PA)

mutable struct PartitioningConstraints
    constraints::Vector{Union{PartitioningConstraint, Function}}
end

PartitioningConstraints() = PartitioningConstraints([])

function to_jl(constraints::PartitioningConstraints)
    return Dict(
        "constraints" =>
            [to_jl(constraint) for constraint in constraints.constraints],
    )
end

###################
# Partition types #
###################

const PartitionTypeParameters = Dict{String, Any}

mutable struct PartitionType
    parameters::PartitionTypeParameters
    constraints::PartitioningConstraints

    PartitionType(
        parameters::Dict{String, <:Any} = PartitionTypeParameters(),
        constraints::PartitioningConstraints = PartitioningConstraints(),
    ) = new(parameters, constraints)
    PartitionType(s::String) = new(Dict("name" => s), PartitioningConstraints())
    PartitionType(parameters::PartitionTypeParameters) = new(parameters, PartitioningConstraints())

    function PartitionType(args::Union{String, Pair{String,<:Any}, PartitioningConstraint, Function}...)
        parameters = Dict()
        constraints = PartitioningConstraints()

        # Construct parameters and constraints from arguments
        for arg in args
            if arg isa String
                parameters["name"] = arg
            elseif arg isa Pair
                parameters[first(arg)] = last(arg)
            elseif arg isa PartitioningConstraint || arg isa Function
                push!(constraints.constraints, arg)
            else
                throw(ArgumentError("Expected either a partition type parameter or constraint"))
            end
        end

        new(parameters, constraints)
    end
end

# We probably need this so we can iterate over PTs produced by Grouped and then
# check the key property
function Base.getproperty(pt::PartitionType, name::Symbol)
    if hasfield(PartitionType, name)
        return getfield(pt, name)
    end

    n = string(name)
    if haskey(pt.parameters, n)
        return pt.parameters[n]
    end
    error("$name not found in partition type parameters")
end

function to_jl(pt::PartitionType)
    # Interpret bangs as random IDs
    for (k, v) in pt.parameters
        if v == "!"
            pt.parameters[k] = generate_bang_value()
        end
    end

    # Construct dictionary
    Dict("parameters" => pt.parameters, "constraints" => to_jl(pt.constraints))
end

##############################
# Partition type composition #
##############################

# This is mutable so that we can append PTs
mutable struct PartitionTypeComposition
    pts::Vector{PartitionType}
end

to_jl(ptc::PartitionTypeComposition) = [to_jl(pt) for pt in ptc.pts]

##############################
# Partition type combinators #
##############################

const PTOrPTUnion = Union{PartitionType,Vector{PartitionType}}

Base.:&(a::PartitionType, b::PartitionType) =
    if all(
        a.parameters[param_name] == b.parameters[param_name] for
        param_name in keys(a.parameters) if param_name in keys(b.parameters)
    )
        [PartitionType(
            merge(a.parameters, b.parameters),
            PartitioningConstraints(
                [a.constraints.constraints; b.constraints.constraints]
            )
        )]
    else
        PartitionType[]
    end

Base.:&(a::Vector{PartitionType}, b::PartitionType)::Vector{PartitionType} =
    vcat([pt & b for pt in a]...)
Base.:&(a::PartitionType, b::Vector{PartitionType}) = b & a
Base.:&(a::Vector{PartitionType}, b::Vector{PartitionType})::Vector{PartitionType} =
    vcat([aa & bb for aa in a for bb in b]...)
Base.:|(a::PTOrPTUnion, b::PTOrPTUnion) = [a; b]

#########################
# Partition annotations #
#########################

# TODO: Rename Partitions to PartitionTypeBinding and keep Partitioning as is
mutable struct Partitions
    # TODO: Only use either PT stack or PT composition to be consistent in
    # terminology
    pt_stacks::Dict{ValueId,PartitionTypeComposition}
end

Partitions() = Partitions(Dict())

function to_jl(p::Partitions)
    # NOTE: This assumes that the PT compositions in `p.pt_stacks` are _not_
    # delayed
    return Dict(
        "pt_stacks" =>
            Dict(v => ptc |> to_jl for (v, ptc) in p.pt_stacks),
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
