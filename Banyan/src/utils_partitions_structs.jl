#############################
# Partition type references #
#############################

const PartitionTypeReference = Tuple{ValueId,Int64}

###################
# Partition types #
###################

struct PartitioningConstraint
    type::String
    args::Vector{PartitionTypeReference}
    co_args::Vector{Vector{PartitionTypeReference}}
    func::Function
end

PartitioningConstraintOverGroup(type, args::Vector{PartitionTypeReference}) =
    PartitioningConstraint(type, args, Vector{PartitionTypeReference}[], identity)
PartitioningConstraintOverGroups(type, co_args::Vector{Vector{PartitionTypeReference}}) =
    PartitioningConstraint(type, PartitionTypeReference[], co_args, identity)
PartitioningConstraintFunction(@nospecialize(func::Function)) =
    PartitioningConstraint("FUNCTION", PartitionTypeReference[], Vector{PartitionTypeReference}[], func)

mutable struct PartitioningConstraints
    constraints::Vector{}
end

PartitioningConstraints() = PartitioningConstraints(PartitioningConstraint[])

const PartitionTypeParameters = Dict{String, Any}

mutable struct PartitionType
    parameters::PartitionTypeParameters
    constraints::PartitioningConstraints

    PartitionType(parameters::PartitionTypeParameters, constraints::PartitioningConstraints) =
        new(parameters, constraints)

    function PartitionType(args::Union{String, Pair{String,<:Any}, PartitioningConstraint, Function}...)
        parameters = Dict{String,Any}()
        constraints = PartitioningConstraints()

        # Construct parameters and constraints from arguments
        for arg in args
            if arg isa String
                parameters["name"] = arg
            elseif arg isa Pair
                parameters[first(arg)] = arg[end]
            elseif arg isa PartitioningConstraint
                push!(constraints.constraints, arg)
            elseif arg isa Function
                push!(constraints.constraints, PartitioningConstraintFunction(arg))
            else
                throw(ArgumentError("Expected either a partition type parameter or constraint"))
            end
        end

        new(parameters, constraints)
    end
end

# This is mutable so that we can append PTs
mutable struct PartitionTypeComposition
    pts::Vector{PartitionType}
end

# TODO: Rename Partitions to PartitionTypeBinding and keep Partitioning as is
mutable struct Partitions
    # TODO: Only use either PT stack or PT composition to be consistent in
    # terminology
    pt_stacks::Dict{ValueId,PartitionTypeComposition}
end

mutable struct PartitionAnnotation
    partitions::Partitions
    constraints::PartitioningConstraints
end

struct PartitionedUsingFunc{K}
    # Memory usage, sampling
    keep_same_sample_rate::Bool
    # Keys (not relevant if you never use grouped partitioning).
    grouped::Vector{Future}
    keep_same_keys::Bool
    keys::Vector{K}
    keys_by_future::Vector{Tuple{Future,Vector{K}}}
    renamed::Bool
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted::Bool
    isnothing::Bool
end

struct PartitionedWithFunc
    func::Function
    future_
end