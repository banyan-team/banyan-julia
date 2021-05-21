# NOTE: Do not construct a PT such that PTs can be fused together or used as-is
# in a way such that there aren't functions for splitting and merging them in
# pt_lib.jl. Note that each splitting and merging function in pt_lib.jl is for
# specific locations and so, for example, a Div should not be used on a value
# with CSV location unless there is a splitting function for that.

Block() = PartitionType(Dict("name" => "Block"))
Block(dim) = PartitionType(Dict("name" => "Block", "dim" => dim))
BlockBalanced() = PartitionType(Dict("name" => "Block", "balanced" => true))
BlockBalanced(dim) =
    PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => true))
BlockUnbalanced() = PartitionType(Dict("name" => "Block", "balanced" => false))
BlockUnbalanced(dim) =
    PartitionType(Dict("name" => "Block", "dim" => dim, "balanced" => false))
    
Div() = PartitionType(Dict("name" => "Replicate", "dividing" => true))
Replicated() = PartitionType(Dict("name" => "Replicate", "replicated" => true))
Reducing(op) = PartitionType(Dict("name" => "Replicate", "replicated" => false, "reducer" => to_jl_value(op)))

# TODO: Generate AtMost and ScaledBy constraints in handling filters and joins
# that introduce data skew and in other operations that explicitly don't

Any = PartitionType(Dict())

Replicating() = PartitionType("name" => "Replicating", "reducer" => nothing)
Replicated() = PartitionType("name" => "Replicating", "replication" => "all", "reducer" => nothing)
Syncing() = PartitionType("name" => "Replicating", "replication" => "one", "reducer" => nothing) # (?)
Reducing(op) = PartitionType("name" => "Replicating", "replication" => "one", "reducer" => to_jl_value(op))

Distributing() = PartitionType("name" => "Distributing")
Drifted() = PartitionType("name" => "Distributing", "id" => "!")
Unbalanced() = PartitionType("name" => "Distributing", "balanced" => false)
Blocked() = PartitionType("name" => "Distributing", "distribution" => "blocked")
Grouped() = PartitionType("name" => "Distributing", "distribution" => "grouped")
Distributed(args...; kwargs...) = Blocked(args...; kwargs...) | Grouped(args...; kwargs...)

function Blocked(
    f::AbstractFuture;
    along = :,
    balanced = nothing,
    filtered_from = nothing,
    filtered_to = nothing,
)
    parameters = Dict()
    constraints = PartitioningConstraints()

    # Prepare `along`
    if along isa Colon
        along = sample(along, :axes)
    end
    along = to_vector(along)
    # TODO: Maybe assert that along isa Vector{String} or Vector{Symbol}

    # Create PTs for each axis that can be used to block along
    pts = []
    for axis in first(4, along)
        parameters = Dict("key" => key)
        constraints = PartitioningConstraints()

        # Handle `balanced`s
        if !isnothing(balanced)
            parameters["balanced"] = balanced
        end
        # if drifted == true
        #     parameters["id"] = "!"
        # end

        # Load divisions
        if balanced == true
            push!(constraints.constraints, ScaleBy(1.0, f))

            # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
            # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
            # that the constraint can be satisfied for this PT to be used
        elseif balanced == false
            # TODO: Support joins
            if !isnothing(filtered_from)
                filtered_from = to_vector(filtered_from)
                factor = maximum(filtered_from) do ff
                    sample(ff, :memory_usage) / sample(f, :memory_usage)
                end
                push!(constraints.constraints, ScaleBy(factor, f, filtered_from))
            elseif !isnothing(filtered_to)
                filtered_to = to_vector(filtered_to)
                factor = maximum(filtered_to) do ft
                    sample(ft, :memory_usage) / sample(f, :memory_usage)
                end
                push!(constraints.constraints, ScaleBy(factor, f, filtered_to))
            end
        end

        push!(pt, PartitionType(parameters, constraints))
    end
    pts
end


function Grouped(
    f::AbstractFuture;
    by = :,
    balanced = nothing,
    filtered_from = nothing,
    filtered_to = nothing,
)
    # Prepare `by`
    if by isa Colon
        by = sample(by, :groupingkeys)
    end
    along = to_vector(along)

    # Create PTs for each key that can be used to group by
    pts = []
    for key in first(by, 8)
        parameters = Dict("key" => key)
        constraints = PartitioningConstraints()

        # Handle `balanced`s
        if !isnothing(balanced)
            parameters["balanced"] = balanced
        end
        # if drifted == true
        #     parameters["id"] = "!"
        # end

        # Load divisions
        if balanced == true
            parameters["divisions"] = sample(f, :statistics, key, :divisions)
            max_ngroups = sample(f, :statistics, key, :max_ngroups)
            push!(constraints.constraints, AtMost(max_ngroups, f))
            push!(constraints.constraints, ScaleBy(1.0, f))

            # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
            # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
            # that the constraint can be satisfied for this PT to be used
        elseif balanced == false
            # TODO: Support joins
            if !isnothing(filtered_from)
                filtered_from = to_vector(filtered_from)
                factor = maximum(filtered_from) do ff
                    f_min = sample(f, :statistics, by, :min)
                    f_max = sample(f, :statistics, by, :max)
                    divisions_filtered_from = sample(ff, :statistics, by, :divisions)
                    f_min_quantile = sample(f, :statistics, :division, f_min)
                    f_max_quantile = sample(f, :statistics, :division, f_max)
                    f_max_quantile - f_min_quantile
                end
                push!(constraints.constraints, ScaleBy(factor, f, filtered_from))
            elseif !isnothing(filtered_to)
                filtered_to = to_vector(filtered_to)
                factor = maximum(filtered_to) do ft
                    min_filtered_to = sample(ft, :statistics, by, :min)
                    max_filtered_to = sample(ft, :statistics, by, :max)
                    f_divisions = sample(f, :statistics, by, :divisions)
                    f_min_quantile = sample(f, :statistics, :division, min_filtered_to)
                    f_max_quantile = sample(f, :statistics, :division, max_filtered_to)
                    1 / (f_max_quantile - f_min_quantile)
                end
                push!(constraints.constraints, ScaleBy(factor, f, filtered_to))
            end
        end

        push!(pt, PartitionType(parameters, constraints))
    end
    pts
end
