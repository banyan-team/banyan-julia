# NOTE: Do not construct a PT such that PTs can be fused together or used as-is
# in a way such that there aren't functions for splitting and merging them in
# pt_lib.jl. Note that each splitting and merging function in pt_lib.jl is for
# specific locations and so, for example, a Div should not be used on a value
# with CSV location unless there is a splitting function for that.

# Block() = PartitionType(Dict("name" => "Block"))
# Block(dim) = PartitionType(Dict("name" => "Block", "dim" => dim))
# BlockBalanced() = PartitionType(Dict("name" => "Block", "balanced" => true))
# BlockBalanced(dim) =
#     PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
# BlockUnbalanced() = PartitionType(Dict("name" => "Block", "balanced" => false))
# BlockUnbalanced(dim) =
#     PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => false))
    
# Div() = PartitionType(Dict("name" => "Replicate", "dividing" => true))
# Replicated() = PartitionType(Dict("name" => "Replicate", "replicated" => true))
# Reducing(op) = PartitionType(Dict("name" => "Replicate", "replicated" => false, "reducer" => to_jl_value(op)))

# TODO: Generate AtMost and ScaledBy constraints in handling filters and joins
# that introduce data skew and in other operations that explicitly don't

Any() = PartitionType(Dict())
Any(;scaled_by_same_as) = PartitionType(f->ScaleBy(1.0, f, scaled_by_same_as))

Replicating() = PartitionType("name" => "Replicating", f->ScaleBy(1.0, f))
Replicated() = Replicating() & PartitionType("replication" => "all", "reducer" => nothing)
# TODO: Add Replicating(f) to the below if needed for reducing operations on
# large objects such as unique(df::DataFrame)

# TODO: Determine whether the `"reducer" => nothing` should be there
Divided() = Replicating() & PartitionType("divided" => true)
Syncing() = Replicating() & PartitionType("replication" => "one", "reducer" => nothing) # TODO: Determine whether this is really needed
Reducing(op) = Replicating() & PartitionType("replication" => nothing, "reducer" => to_jl_value(op), "with_key"=false)
ReducingWithKey(op) = Replicating() & PartitionType("replication" => nothing, "reducer" => to_jl_value(op), "with_key"=true)
# TODO: Maybe replace banyan_reduce_size_by_key with an anonymous function since that actually _can_ be ser/de-ed
# or instead make there be a reducing type that passes in the key to the reducing functions so it can reduce by that key
# ReducingSize() = PartitionType("replication" => "one", "reducer" => "banyan_reduce_size_by_key")

Distributing() = PartitionType("name" => "Distributing")
Blocked() = PartitionType("name" => "Distributing", "distribution" => "blocked")
Blocked(;along) = PartitionType("name" => "Distributing", "distribution" => "blocked", "key" => along)
Grouped() = PartitionType("name" => "Distributing", "distribution" => "grouped")
# Blocked(;balanced) = PartitionType("name" => "Distributing", "distribution" => "blocked", "balanced" => balanced)
# Grouped(;balanced) = PartitionType("name" => "Distributing", "distribution" => "grouped", "balanced" => balanced)

ScaledBySame(as) = Distributing() & PartitionType(f->ScaleBy(1.0, f, as))
Drifted() = Distributing() & PartitionType("id" => "!")
Balanced() = Distributing() & PartitionType("balanced" => true, f->ScaleBy(1.0, f))
Unbalanced() = Distributing() & PartitionType("balanced" => false)
Unbalanced(;scaled_by_same_as) = Unbalanced() & ScaledBySame(as=scaled_by_same_as)

# These functions (along with `keep_sample_rate`) allow for managing memory
# usage in annotated code. `keep_sample_rate` allows for setting the sample
# rate as it changes from value to value. Some operations such as joins
# actually require a change in sample rate so propagating this information is
# important and must be done before partition annotations are applied (in
# `partitioned_using`). In the partition annotation itself, we sometimes want
# to set constraints on how we scale the memory usage based on how much skew
# is introduced by an operation. Some operations not only change the sample
# rate but also introduce skew and so applying these constraints is important.
# FilteredTo and FilteredFrom help with constraining skew when it is introduced
# through data filtering operations while MutatedTo and MutatedFrom allow for
# propagatng skew for operations where the skew is unchanged. Balanced data
# doesn't have any skew and Balanced and balanced=true help to make this clear.
# TODO: Remove this if we don't need
# MutatedRelativeTo(f, mutated_relative_to) = PartitionType(ScaleBy(1.0, f, mutated_relative_to))
# MutatedTo(f, mutated_to) = MutatedRelativeTo(f, mutated_to)
# MutatedFrom(f, mutated_from) = MutatedRelativeTo(f, mutated_from)

Distributed(args...; kwargs...) = Blocked(args...; kwargs...) | Grouped(args...; kwargs...)
Partitioned(args...; kwargs...) = Distributed(args...; kwargs...) | Replicated()

function Blocked(
    f::AbstractFuture;
    along = :,
    balanced = nothing,
    filtered_from = nothing,
    filtered_to = nothing,
    scaled_by_same_as = nothing,
)
    parameters = Dict()
    constraints = PartitioningConstraints()

    # Prepare `along`
    if along isa Colon
        along = sample(along, :axes)
        # TODO: Ensure that axes returns [1] for DataFrame and axes for Array
        # while keys returns keys for DataFrame and axes for Array
    end
    along = to_vector(along)
    # TODO: Maybe assert that along isa Vector{String} or Vector{Symbol}

    # Create PTs for each axis that can be used to block along
    pts = []
    for axis in first(4, along)
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in isnothing(balanced) ? [true, false] : [balanced]
            # Initialize parameters
            parameters = Dict("key" => key, "balanced" => b)
            constraints = PartitioningConstraints()

            # Create `ScaleBy` constraints
            if b
                push!(constraints.constraints, ScaleBy(1.0, f))
                # TODO: Add an AtMost constraint in the case that input elements are very large
            else
                if !isnothing(filtered_from)
                    filtered_from = to_vector(filtered_from)
                    factor, from = maximum(filtered_from) do ff
                        (sample(ff, :memory_usage) / sample(f, :memory_usage), filtered_from)
                    end
                    push!(constraints.constraints, ScaleBy(factor, f, from))
                elseif !isnothing(filtered_to)
                    filtered_to = to_vector(filtered_to)
                    factor, to = maximum(filtered_to) do ft
                        (sample(ft, :memory_usage) / sample(f, :memory_usage), filtered_to)
                    end
                    push!(constraints.constraints, ScaleBy(factor, f, to))
                elseif !isnothing(scaled_by_same_as)
                    push!(constraints.constraints, ScaleBy(1.0, f, scaled_by_same_as))
                end
            end

            # Append new PT to PT union being produced
            push!(pt, PartitionType(parameters, constraints))
        end
    end

    # Return the resulting PT union that can then be passed into a call to `pt`
    # which would in turn result in a PA union
    pts
end

# NOTE: A reason to use Grouped for element-wise computation (with no
# filtering) is to allow for the input to be re-balanced. If you just use
# Any then there wouldn't be any way to re-balance right before the
# computation. Grouped allows the input to have either balanced=true or
# balanced=false and if balanced=true is chosen then a cast may be applied.

function Grouped(
    f::AbstractFuture;
    # Parameters for splitting into groups
    by = :,
    balanced = nothing,
    rev = nothing,
    # Options to deal with skew
    filtered_from = nothing,
    filtered_to = nothing,
    scaled_by_same_as = nothing,
)
    # Prepare `by`
    if by isa Colon
        by = sample(by, :groupingkeys)
    end
    by = Symbol(by)
    along = to_vector(along)

    # Create PTs for each key that can be used to group by
    pts = []
    for key in first(by, 8)
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in isnothing(balanced) ? [true, false] : [balanced]
            parameters = Dict("key" => key, "balanaced" => b)
            constraints = PartitioningConstraints()

            # Create `ScaleBy` constraint and also compute `divisions` and
            # `AtMost` constraint if balanced
            if b
                # Set divisions
                # TODO: Change this if `divisions` is not a `Vector{Tuple{Any,Any}}`
                parameters["divisions"] = to_jl_value(sample(f, :statistics, key, :divisions))
                max_ngroups = sample(f, :statistics, key, :max_ngroups)

                # Set flag for reversing the order of the groups
                if !isnothing(rev)
                    parameters["rev"] = rev
                end

                # Add constraints
                push!(constraints.constraints, AtMost(max_ngroups, f))
                push!(constraints.constraints, ScaleBy(1.0, f))

                # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
                # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
                # that the constraint can be satisfied for this PT to be used
            else
                # TODO: Support joins
                if !isnothing(filtered_from)
                    filtered_from = to_vector(filtered_from)
                    factor, from = maximum(filtered_from) do ff
                        f_min = sample(f, :statistics, by, :min)
                        f_max = sample(f, :statistics, by, :max)
                        divisions_filtered_from = sample(ff, :statistics, by, :divisions)
                        f_percentile = sample(f, :statistics, :percentile, min_filtered_to, max_filtered_to)
                        (f_percentile, filtered_from)
                    end
                    push!(constraints.constraints, ScaleBy(factor, f, from))
                elseif !isnothing(filtered_to)
                    filtered_to = to_vector(filtered_to)
                    factor, to = maximum(filtered_to) do ft
                        min_filtered_to = sample(ft, :statistics, by, :min)
                        max_filtered_to = sample(ft, :statistics, by, :max)
                        f_divisions = sample(f, :statistics, by, :divisions)
                        f_percentile = sample(f, :statistics, :percentile, min_filtered_to, max_filtered_to)
                        (1 / f_percentile, filtered_to)
                    end
                    push!(constraints.constraints, ScaleBy(factor, f, to))
                elseif !isnothing(scaled_by_same_as)
                    push!(constraints.constraints, ScaleBy(1.0, f, scaled_by_same_as))
                end
            end
        end

        push!(pt, PartitionType(parameters, constraints))
    end
    pts
end
