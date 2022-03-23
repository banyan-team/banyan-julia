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

# Don't specialize because we anyway have typechecks inside each function
@nospecialize

Replicating() = PartitionType("name" => "Replicating", f->Scale(f, by=1.0))
Replicated()::Vector{PartitionType} = Replicating() & PartitionType("replication" => "all")
# TODO: Add Replicating(f) to the below if needed for reducing operations on
# large objects such as unique(df::DataFrame)

# TODO: Determine whether the `"reducer" => nothing` should be there
Divided()::Vector{PartitionType} = Replicating() & PartitionType("dividing" => true)
Reducing(op::Union{Function,Expr})::Vector{PartitionType} = Replicating() & PartitionType("replication" => nothing, "reducer" => to_jl_value(op), "with_key" => false)
ReducingWithKey(op::Function)::Vector{PartitionType} = Replicating() & PartitionType("replication" => nothing, "reducer" => to_jl_value(op), "with_key" => true)
# TODO: Maybe replace banyan_reduce_size_by_key with an anonymous function since that actually _can_ be ser/de-ed
# or instead make there be a reducing type that passes in the key to the reducing functions so it can reduce by that key
# ReducingSize() = PartitionType("replication" => "one", "reducer" => "banyan_reduce_size_by_key")

Distributing() = PartitionType("name" => "Distributing")
Blocked(; along = nothing)::PartitionType =
    if isnothing(along)
        PartitionType("name" => "Distributing", "distribution" => "blocked")
    else
        PartitionType(
            "name" => "Distributing",
            "distribution" => "blocked",
            "key" => along,
        )
    end
Grouped() = PartitionType("name" => "Distributing", "distribution" => "grouped")
# Blocked(;balanced) = PartitionType("name" => "Distributing", "distribution" => "blocked", "balanced" => balanced)
# Grouped(;balanced) = PartitionType("name" => "Distributing", "distribution" => "grouped", "balanced" => balanced)

ScaledBySame(;as) = PartitionType(f -> Scale(f, by=1.0, relative_to=Future[convert(Future, as)]))
Drifted()::Vector{PartitionType} = Distributing() & PartitionType("id" => "!")
Balanced()::Vector{PartitionType} =
    Distributing() & PartitionType("balanced" => true, f -> Scale(f, by=1.0))
Unbalanced(; scaled_by_same_as::Union{<:AbstractFuture,Nothing} = nothing)::Vector{PartitionType} =
    if isnothing(scaled_by_same_as)
        Distributing() & PartitionType("balanced" => false)
    else
        Unbalanced() & ScaledBySame(as = scaled_by_same_as)
    end

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

Distributed(args...; kwargs...)::Vector{PartitionType} = Blocked(args...; kwargs...) | Grouped(args...; kwargs...)
Partitioned(args...; kwargs...)::Vector{PartitionType} = Distributed(args...; kwargs...) | Replicated()

function Blocked(
    f::AbstractFuture;
    along::Union{Colon,Vector{Int64},Int64} = :,
    balanced::Union{Nothing,Bool} = nothing,
    filtered_from::Union{Nothing,AbstractFuture} = nothing,
    filtered_to::Union{Nothing,AbstractFuture} = nothing,
    scaled_by_same_as::Union{AbstractFuture,Nothing} = nothing,
)::Vector{PartitionType}
    f = convert(Future, f)::Future

    # Prepare `along`
    along = if along isa Colon
        sample_axes(sample(f))::Vector{Int64}
    elseif along isa Vector
        along
    else
        Int64[along]
    end
    # TODO: Ensure that axes returns [1] for DataFrame and axes for Array
    # while keys returns keys for DataFrame and axes for Array
    # TODO: Maybe assert that along isa Vector{String} or Vector{Symbol}

    balanced = isnothing(balanced) ? Bool[true, false] : Bool[balanced]

    filtered_from::Vector{Future} =
        isnothing(filtered_from) ? Future[] : Future[convert(Future, filtered_from)]
    filtered_to::Vector{Future} =
        isnothing(filtered_to) ? Future[] : Future[convert(Future, filtered_to)]
    scaled_by_same_as::Vector{Future} =
        isnothing(scaled_by_same_as) ? Future[] : Future[convert(Future, scaled_by_same_as)]

    # Create PTs for each axis that can be used to block along
    Blocked(f, along, balanced, filtered_from, filtered_to, scaled_by_same_as)
end

function Blocked(
    f::Future,
    along::Vector{Int64},
    balanced::Vector{Bool},
    filtered_from::Vector{Future},
    filtered_to::Vector{Future},
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType}
    f = convert(Future, f)::Future

    parameters = Dict{String,Any}()
    constraints = PartitioningConstraints()

    # # Prepare `along`
    # K = eltype(sample_keys(sample(f)))
    # if along isa Colon
    #     along = sample(f, :axes)
    #     # TODO: Ensure that axes returns [1] for DataFrame and axes for Array
    #     # while keys returns keys for DataFrame and axes for Array
    # end
    # along = to_vector(along)
    # # TODO: ;Maybe assert that along isa Vector{String} or Vector{Symbol}

    # Create PTs for each axis that can be used to block along
    pts::Vector{PartitionType} = PartitionType[]
    for axis in along[1:min(4,end)]
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced
            # Initialize parameters
            parameters::Dict{String,Any} = Dict("key" => axis, "balanced" => b)
            constraints = PartitioningConstraints()

            # Create `ScaleBy` constraints
            if b
                push!(constraints.constraints, Scale(f, by=1.0))
                # TODO: Add an AtMost constraint in the case that there are very few rows.
                # That AtMost constraint would only go here in Blocked
            else
                factor::Float64 = -1
                if !isempty(filtered_from)
                    # If 100 elements get filtered to 20 elements and the
                    # original data was block-partitioned in a balanced
                    # way, the result may all be on one partition in the
                    # msot extreme case (not balanced at all) and so we
                    # should adjust the memory usage of the result by
                    # multiplying it by the size of the original / the size
                    # of the result (100 / 20 = 5).
                    for ff in filtered_from
                        ff_factor::Float64 = sample(ff, :memory_usage)::Int64 / sample(f, :memory_usage)::Int64
                        if ff_factor > factor
                            factor = ff_factor
                        end
                    end
                    factor != -1 || error("Factor of scaling should not be -1")

                    # The factor will be infinite or NaN if either what we are
                    # filtering from or filtering to has an empty sample. In
                    # that case, it wouldn't make sense to have a `ScaleBy`
                    # constraint. A value with an empty sample must either be
                    # replicated (if it is from an empty dataset) or
                    # grouped/blocked but not balanced (since if it were
                    # balanced, we might try to use its divisions - which would
                    # be empty - for other PTs and think that they to are balanced).
                    if factor != Inf && factor != NaN
                        push!(constraints.constraints, Scale(f, by=factor, relative_to=filtered_from))
                    end
                elseif !isempty(filtered_to)
                    for ft in filtered_to
                        ft_factor::Float64 = sample(f, :memory_usage)::Int64 / sample(ft, :memory_usage)::Int64
                        if ft_factor > factor
                            factor = ft_factor
                        end
                    end

                    # The factor will be infinite or NaN if either what we are
                    # filtering from or filtering to has an empty sample. In
                    # that case, it wouldn't make sense to have a `ScaleBy`
                    # constraint. A value with an empty sample must either be
                    # replicated (if it is from an empty dataset) or
                    # grouped/blocked but not balanced (since if it were
                    # balanced, we might try to use its divisions - which would
                    # be empty - for other PTs and think that they to are balanced).
                    if factor != Inf && factor != NaN
                        push!(constraints.constraints, Scale(f, by=factor, relative_to=filtered_to))
                    end
                elseif !isempty(scaled_by_same_as)
                    push!(constraints.constraints, Scale(f, by=1.0, relative_to=scaled_by_same_as))
                end
            end

            # Append new PT to PT union being produced
            push!(pts, PartitionType(parameters, constraints))
        end
    end

    # Return the resulting PT union that can then be passed into a call to `pt`
    # which would in turn result in a PA union
    Blocked() & pts
end

# NOTE: A reason to use Grouped for element-wise computation (with no
# filtering) is to allow for the input to be re-balanced. If you just use
# Any then there wouldn't be any way to re-balance right before the
# computation. Grouped allows the input to have either balanced=true or
# balanced=false and if balanced=true is chosen then a cast may be applied.

const FutureByOptionalKey{K} = Dict{Future,Union{K,Nothing}}

function Grouped(
    f::AbstractFuture;
    # Parameters for splitting into groups
    by::Union{Nothing,Colon,T,Vector{T}} = nothing,
    balanced::Union{Nothing,Bool} = nothing,
    rev::Union{Nothing,Bool} = nothing,
    # Options to deal with skew
    filtered_from::Union{Nothing,AbstractFuture,Dict{<:AbstractFuture,T}} = nothing,
    filtered_to::Union{Nothing,AbstractFuture,Dict{<:AbstractFuture,T}} = nothing,
    scaled_by_same_as::Union{AbstractFuture,Nothing} = nothing,
)::Vector{PartitionType} where {T}
    f = convert(Future, f)::Future

    K = if by isa Union{Nothing,Colon} && filtered_from isa Union{Nothing,AbstractFuture} && filtered_to isa Union{Nothing,AbstractFuture}
        eltype(sample_keys(sample(f)))
    else
        T
    end

    # Prepare `by`
    by::Vector{K} = if isnothing(by)
        sample(f, :groupingkeys)::Vector{K}
    elseif by isa Colon
        sample(f, :keys)::Vector{K}
    elseif by isa Vector
        by
    else
        K[by]
    end
    intersect!(by, sample(f, :keys)::Vector{K})
    by = by[1:min(8,end)]

    balanced = isnothing(balanced) ? Bool[true, false] : Bool[balanced]

    filtered_from::FutureByOptionalKey{K} =
        if isnothing(filtered_from)
            FutureByOptionalKey{K}()
        elseif filtered_from isa Dict
            FutureByOptionalKey{K}(
                convert(Future, f) => key
                for (f, key) in filtered_from
            )
        else
            FutureByOptionalKey{K}(convert(Future, filtered_from) => nothing)
        end
    filtered_to::FutureByOptionalKey{K} =
        if isnothing(filtered_to)
            FutureByOptionalKey{K}()
        elseif filtered_to isa Dict
            FutureByOptionalKey{K}(
                convert(Future, f) => key
                for (f, key) in filtered_to
            )
        else
            FutureByOptionalKey{K}(convert(Future, filtered_to) => nothing)
        end
    scaled_by_same_as::Vector{Future} =
        isnothing(scaled_by_same_as) ? Future[] : Future[convert(Future, scaled_by_same_as)]

    Grouped(
        f,
        by,
        balanced,
        rev,
        filtered_from,
        filtered_to,
        scaled_by_same_as,
    )
end

function Grouped(
    f::Future,
    by::Vector{K},
    balanced::Vector{Bool},
    rev::Union{Nothing,Bool},
    filtered_from::FutureByOptionalKey{K},
    filtered_to::FutureByOptionalKey{K},
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType} where K
    # Create PTs for each key that can be used to group by
    pts::Vector{PartitionType} = []
    for (i, key) in enumerate(by)
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced
            parameters::Dict{String,Any} = Dict("key" => key, "balanced" => b)
            constraints = PartitioningConstraints()

            # Create `ScaleBy` constraint and also compute `divisions` and
            # `AtMost` constraint if balanced
            if b
                # Set divisions
                # TODO: Change this if `divisions` is not a `Vector{Tuple{Any,Any}}`
                parameters["divisions"] = to_jl_value_contents(sample(f, :statistics, key, :divisions))
                max_ngroups = sample(f, :statistics, key, :max_ngroups)::Int64

                # Set flag for reversing the order of the groups
                if !isnothing(rev)
                    rev::Bool
                    parameters["rev"] = rev
                end

                # Add constraints
                # In the future if the element size can be really big (like a multi-dimensional array
                # that is very wide or data frame with many columns), we may want to have a constraint
                # where the partition size must be larger than that minimum element size.

                # Note that if the sample is empty, the maximum # of groups
                # will be zero and so this AtMost constraint will cause the PA
                # to fail to be used. This is expected. You can't have a PA
                # where empty data is balanced. If the empty data arises
                # because of an empty dataset being queried/processed, we
                # should be using replication. If the empty data arises because
                # of highly selective filtering, we will filter from some data
                # that _is_ balanced.
                push!(constraints.constraints, AtMost(max_ngroups, f))
                push!(constraints.constraints, Scale(f, by=1.0))

                # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
                # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
                # that the constraint can be satisfied for this PT to be used
            else
                # TODO: Support joins
                factor::Float64 = 0
                if !isempty(filtered_from)
                    from = Base.collect(keys(filtered_from))
                    for (ff, fby) in filtered_from
                        # Get key to use for filtering
                        fkey::K = isnothing(fby) ? key : fby

                        # IF what wea re filtering to is empty, we don't know
                        # anything about the skew of data being filtered.
                        # Everything could be in a single partition or evenly
                        # distributed and the result would be the same. If this
                        # PT is ever fused with some matching PT that _is_
                        # balanced and has divisions specified, then that will
                        # be becasue of some filtering going on in which case a
                        # `ScaleBy` constraint will be enforced.

                        # Handling empty data:
                        # If empty data is considered balanced, we should
                        # replicate everything. Otherwise, there should be some
                        # other data in the filtering pipeline that is
                        # balanced.
                        # - disallow balanced grouping of empty dataset
                        # - filtering to an empty dataset - no ScaleBy constraint
                        # - filtering from an empty dataset - no ScaleBy constraint
                        # - maybe add in a ScaleBy(-1j) to prevent usage of an empty data

                        # Compute the amount to scale memory usage by based on data skew
                        min_filtered_to = sample(f, :statistics, key, :min)
                        max_filtered_to = sample(f, :statistics, key, :max)
                        # divisions_filtered_from = sample(ff, :statistics, key, :divisions)
                        ff_percentile = sample(ff, :statistics, fkey, :percentile, min_filtered_to, max_filtered_to)::Float64
                        ffactor::Float64 = 1 / ff_percentile
                        if ffactor > factor
                            factor = ffactor
                        end
                    end

                    # If the sample of what we are filtering into is empty, the
                    # factor will be infinity. In that case, we shouldn't be
                    # creating a ScaleBy constraint.
                    if factor != Inf
                        push!(constraints.constraints, Scale(f, by=factor, relative_to=from))
                    end
                elseif !isempty(filtered_to)
                    # TODO: Revisit this and ensure it's okay to just take the
                    # maximum and we don't have to actually multiply all of the
                    # factors by which this fails to scale.
                    to = Base.collect(keys(filtered_to))
                    for (ft, fby) in filtered_to
                        fkey::K = isnothing(fby) ? key : fby

                        # Compute the amount to scale memory usage by based on data skew
                        min_filtered_to = sample(ft, :statistics, fkey, :min)
                        max_filtered_to = sample(ft, :statistics, fkey, :max)
                        # f_divisions = sample(f, :statistics, key, :divisions)
                        f_percentile = sample(f, :statistics, key, :percentile, min_filtered_to, max_filtered_to)::Float64
                        ffactor::Float64 = 1 / f_percentile
                        if ffactor > factor
                            factor = ffactor
                        end
                    end
                    # TODO: Return all filtered_from/filtered_to but with the appropriate factor and without pairs

                    # If the sample of what we are filtering into is empty, the
                    # factor will be infinity. In that case, we shouldn't be
                    # creating a ScaleBy constraint.
                    if factor != Inf
                        push!(constraints.constraints, Scale(f, by=factor, relative_to=to))
                    end
                elseif !isempty(scaled_by_same_as)
                    push!(constraints.constraints, Scale(f, by=1.0, relative_to=scaled_by_same_as))
                end
            end

            push!(pts, PartitionType(parameters, constraints))
        end
    end

    # If there are no PTs, ensure that we at least have one impossible PT. This
    # PT doesn't need to have divisions and can be unbalanced but the important
    # thing is that we assign an AtMost-zero constraint which will prevent a PA
    # containing this from being used. This is important because we can't group
    # data on keys that don't belong to it.
    if isempty(pts)
        push!(pts, PartitionType("key" => nothing, "balanced" => false, f -> AtMost(0, f)))
    end

    Grouped() & pts
end

@specialize
