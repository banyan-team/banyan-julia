############################
# Partitioning constraints #
############################

# pt_ref_to_jl(pt_ref::Tuple{Future,Int64}) = (pt_ref[1].value_id, pt_ref[2])
pt_ref_to_jl(f::Future) = (f.value_id, 0)

# pt_ref_to_jl(pt_ref::Tuple{<:AbstractFuture,Int64}) =
#     if pt_ref isa Tuple
#         (convert(Future, pt_ref[1]).value_id, pt_ref[2] - 1)
#     else
#         (convert(Future, pt_ref).value_id, 0)
#     end
# pt_ref_to_jl(f::Future) = (f.value_id, 0)

pt_refs_to_jl(refs::Vector{Future}) = map(pt_ref_to_jl, refs)

to_jl(pc::PartitioningConstraint)::Dict{String,Any} =
    Dict{String,Any}("type" => pc.type, "args" => isempty(pc.co_args) ? pc.args : pc.co_cargs)

# arg_to_jl_for_co(arg) =
#     if arg isa Vector
#         pt_refs_to_jl(arg)
#     else
#         [pt_ref_to_jl(arg)]
#     end

# function constraint_for_co(args)::PartitioningConstraintOverGroups
#     if any(arg isa Vector for arg in args)
#         args = [arg_to_jl_for_co(arg) for arg in args]
#         PartitioningConstraintOverGroups("CO_GROUP", args)
#     else
#         PartitioningConstraintOverGroups("CO", pt_refs_to_jl(args))
#     end
# end

# TODO: Support Ordered
# Co(args...) = constraint_for_co(args)
Cross(args::Vector{Future}) = PartitioningConstraintOverGroup("CROSS", pt_refs_to_jl(args))
# Equal(args...) = PartitioningConstraintOverGroup("EQUAL", pt_refs_to_jl(args))
# Sequential(args...) =
#     PartitioningConstraintOverGroup("SEQUENTIAL", pt_refs_to_jl(args))
# Note that a Match constraint will result in the same PT-level constraints
# being applied to all matching PTs
Match(args::Vector{Future}) = PartitioningConstraintOverGroup("MATCH", pt_refs_to_jl(args))
MatchOn(on::String, args::Vector{Future}) =
    PartitioningConstraintOverGroup(
        "MATCH_ON=" * string(on),
        pt_refs_to_jl(args),
    )
AtMost(npartitions::Int64, f::Future) =
    PartitioningConstraintOverGroup(
        "AT_MOST=$npartitions",
        PartitionTypeReference[pt_ref_to_jl(f)]
    )
# ScaleBy(arg, factor::Real = 1.0, relative_to...) = 
#     PartitioningConstraintOverGroup(
#         "SCALE_BY=$factor",
#         pt_refs_to_jl([arg; relative_to...])
#     )
# If used inside a PT constructor as a PT-level constraint, note that
# PT-level constraints are applicable only for the first PT in a value's
# assigned composition of PTs.
# So for example, if you
# are having to scale data by a certain amount to account for data skew when
# grouping, you should be having the same grouping PT throughout the PT
# composition.
Scale(arg::Future; to::Union{Real,String,Nothing}=nothing, by::Real=1.0, relative_to::Vector{Future}=Future[]) =
    Scale(arg, isnothing(to) ? -1.0 : parse_bytes(to), convert(Float64, by)::Float64, relative_to)
function Scale(arg::Future, to::Float64, by::Float64, relative_to::Vector{Future})
    new_relative_to::Vector{Future} = copy(relative_to)
    push!(new_relative_to, arg)
    PartitioningConstraintOverGroup(
        to < 0.0 ? "SCALE_BY=$by" : "SCALE_TO=$to",
        pt_refs_to_jl(new_relative_to)
    )
end
# Fixed(args...) = PartitioningConstraintOverGroup("FIXED", pt_refs_to_jl(args))

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

function to_jl(constraints::PartitioningConstraints)::Dict{String,Any}
    return Dict{String,Any}(
        "constraints" =>
            Dict{String,Any}[
                to_jl(constraint)
                for constraint in constraints.constraints
            ],
    )
end

# # We probably need this so we can iterate over PTs produced by Grouped and then
# # check the key property
# function Base.getproperty(pt::PartitionType, name::Symbol)
#     if hasfield(PartitionType, name)
#         return getfield(pt, name)
#     end

#     n = string(name)
#     haskey(pt.parameters, n) || error("$name not found in partition type parameters")
#     pt.parameters[n]
# end

function to_jl(pt::PartitionType)::Dict{String,Any}
    # Interpret bangs as random IDs
    for (k, v) in pt.parameters
        if v == "!"
            pt.parameters[k] = generate_bang_value()
        end
    end

    # Construct dictionary
    Dict{String,Any}("parameters" => pt.parameters, "constraints" => to_jl(pt.constraints))
end

##############################
# Partition type composition #
##############################

to_jl(ptc::PartitionTypeComposition)::Vector{Dict{String,Any}} = map(to_jl, ptc.pts)

##############################
# Partition type combinators #
##############################

const PTOrPTUnion = Union{PartitionType,Vector{PartitionType}}

function merge_pts!(a::PartitionType, b::PartitionType, pts_so_far::Vector{PartitionType})
    all_params_matching = true
    for param_name in keys(a.parameters)
        if haskey(b.parameters, param_name)
            all_params_matching = all_params_matching && a.parameters[param_name] == b.parameters[param_name]
        end
        if !all_params_matching
            break
        end
    end
    if all_params_matching
        push!(
            pts_so_far,
            PartitionType(
                merge(a.parameters, b.parameters),
                PartitioningConstraints(
                    vcat(a.constraints.constraints, b.constraints.constraints)
                )
            )
        )
    end
end

function Base.:&(a::PartitionType, b::PartitionType)::Vector{PartitionType}
    res::Vector{PartitionType} = PartitionType[]
    merge_pts!(a, b, res)
    res
end

function Base.:&(a::Vector{PartitionType}, b_pt::PartitionType)::Vector{PartitionType}
    res::Vector{PartitionType} = PartitionType[]
    for a_pt in a
        merge_pts!(a_pt, b_pt, res)
    end
    res
end
function Base.:&(a::Vector{PartitionType}, b::Vector{PartitionType})::Vector{PartitionType}
    res::Vector{PartitionType} = PartitionType[]
    for a_pt in a
        for b_pt in b
            merge_pts!(a_pt, b_pt, res)
        end
    end
    res
end
Base.:&(a::PartitionType, b::Vector{PartitionType})::Vector{PartitionType} = b & a
Base.:|(a::PTOrPTUnion, b::PTOrPTUnion)::Vector{PartitionType} = vcat(a, b)

#########################
# Partition annotations #
#########################

Partitions() = Partitions(Dict{ValueId,PartitionTypeComposition}())

function to_jl(p::Partitions)::Dict{String,Any}
    # NOTE: This assumes that the PT compositions in `p.pt_stacks` are _not_
    # delayed
    return Dict{String,Any}(
        "pt_stacks" =>
        Dict{String,Any}(v => to_jl(ptc) for (v, ptc) in p.pt_stacks),
    )
end

PartitionAnnotation() = PartitionAnnotation(Partitions(), PartitioningConstraints())

function to_jl(pa::PartitionAnnotation)::Dict{String,Any}
    return Dict{String,Any}(
        "partitions" => to_jl(pa.partitions),
        "constraints" => to_jl(pa.constraints),
    )
end

const NOTHING_PARTITIONED_USING_FUNC = PartitionedUsingFunc{Int64}(
    false,
    Future[],
    false,
    Int64[],
    Dict{Future,Vector{Int64}}(),
    false,
    false,
    true
)

Base.isnothing(f::PartitionedUsingFunc) = f.isnothing