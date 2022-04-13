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

noscale(f) = Scale(f, by=1.0)
const REPLICATING = PartitionType("name" => "Replicating", noscale)
Replicating()::PartitionType = deepcopy(REPLICATING)
const REPLICATED = PartitionType("name" => "Replicating", noscale, "replication" => "all")
Replicated()::PartitionType = deepcopy(REPLICATED)
# TODO: Add Replicating(f) to the below if needed for reducing operations on
# large objects such as unique(df::DataFrame)

# TODO: Determine whether the `"reducer" => nothing` should be there
const DIVIDED = PartitionType("name" => "Replicating", noscale, "dividing" => true)
Divided()::PartitionType = deepcopy(DIVIDED)
Reducing(op::Function)::PartitionType =
    PartitionType("name" => "Replicating", noscale, "replication" => nothing, "reducer" => to_jl_value(op), "with_key" => false)
ReducingWithKey(op::Function)::PartitionType =
    PartitionType("name" => "Replicating", noscale, "replication" => nothing, "reducer" => to_jl_value(op), "with_key" => true)
# TODO: Maybe replace banyan_reduce_size_by_key with an anonymous function since that actually _can_ be ser/de-ed
# or instead make there be a reducing type that passes in the key to the reducing functions so it can reduce by that key
# ReducingSize() = PartitionType("replication" => "one", "reducer" => "banyan_reduce_size_by_key")

Distributing()::PartitionType = PartitionType("name" => "Distributing")
const BLOCKED = PartitionType("name" => "Distributing", "distribution" => "blocked")
Blocked()::PartitionType = deepcopy(BLOCKED)
Blocked(along::Int64)::PartitionType =
    PartitionType(
        "name" => "Distributing",
        "distribution" => "blocked",
        "key" => along,
    )
const GROUPED = PartitionType("name" => "Distributing", "distribution" => "grouped")
Grouped()::PartitionType = deepcopy(GROUPED)
# Blocked(;balanced) = PartitionType("name" => "Distributing", "distribution" => "blocked", "balanced" => balanced)
# Grouped(;balanced) = PartitionType("name" => "Distributing", "distribution" => "grouped", "balanced" => balanced)

scale_by_future(as::Future) = f -> Scale(f, by=1.0, relative_to=Future[as])
ScaledBySame(as::AbstractFuture)::PartitionType = PartitionType(scale_by_future(convert(Future, as)::Future))
const DRIFTED = PartitionType("name" => "Distributing", "id" => "!")
Drifted()::PartitionType = deepcopy(DRIFTED)
const BALANCED = PartitionType("name" => "Distributing", "balanced" => true, noscale)
Balanced()::PartitionType = deepcopy(BALANCED)
const UNBALANCED = PartitionType("name" => "Distributing", "balanced" => false)
Unbalanced()::PartitionType = deepcopy(UNBALANCED)
Unbalanced(scaled_by_same_as::AbstractFuture)::PartitionType =
    PartitionType("name" => "Distributing", "balanced" => false, scale_by_future(convert(Future, scaled_by_same_as)::Future))

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

function Distributed(
    f::AbstractFuture;
    balanced::Union{Nothing,Bool} = nothing,
    filtered_from::Union{Nothing,AbstractFuture} = nothing,
    filtered_to::Union{Nothing,AbstractFuture} = nothing,
    scaled_by_same_as::Union{AbstractFuture,Nothing} = nothing,
)::Vector{PartitionType}
    blocked_pts::Vector{PartitionType} = Blocked(
        f,
        balanced=balanced,
        filtered_from=filtered_from,
        filtered_to=filtered_to,
        scaled_by_same_as=scaled_by_same_as
    )
    grouped_pts::Vector{PartitionType} = Grouped(
        f,
        balanced=balanced,
        filtered_from=filtered_from,
        filtered_to=filtered_to,
        scaled_by_same_as=scaled_by_same_as
    )
    vcat(blocked_pts, grouped_pts)
end

Partitioned(f::AbstractFuture) = begin
    res = Distributed(f)
    push!(res, Replicated())
    res
end

function make_blocked_pt(
    f::Future,
    axis::Int64,
    b::Bool,
    filtered_from::Vector{Future},
    filtered_to::Vector{Future},
    scaled_by_same_as::Vector{Future},
)::PartitionType
    # Initialize parameters
    new_pt = Blocked()
    parameters::Dict{String,Any} = new_pt.parameters
    parameters["key"] = axis
    parameters["balanced"] = b
    constraints = new_pt.constraints
    f_s::Sample = get_location(f).sample

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
                ff_factor::Float64 = get_location(ff).sample.memory_usage / f_s.memory_usage
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
                ft_factor::Float64 = f_s.memory_usage / get_location(ft).sample.memory_usage
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
    
    new_pt
end

function Blocked(
    f::Future,
    along::Vector{Int64},
    balanced::Vector{Bool},
    filtered_from::Vector{Future},
    filtered_to::Vector{Future},
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType}

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
            # Append new PT to PT union being produced
            push!(pts, make_blocked_pt(f, axis, b, filtered_from, filtered_to, scaled_by_same_as))
        end
    end

    # Return the resulting PT union that can then be passed into a call to `pt`
    # which would in turn result in a PA union
    pts
end

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
    along_res::Vector{Int64} = if along isa Colon
        sample_axes(sample(f))::Vector{Int64}
    elseif along isa Vector
        along
    else
        Int64[along]
    end
    # TODO: Ensure that axes returns [1] for DataFrame and axes for Array
    # while keys returns keys for DataFrame and axes for Array
    # TODO: Maybe assert that along isa Vector{String} or Vector{Symbol}

    balanced_res::Vector{Bool} = isnothing(balanced) ? Bool[true, false] : Bool[balanced]

    filtered_from_res::Vector{Future} =
        isnothing(filtered_from) ? Future[] : Future[convert(Future, filtered_from)]
    filtered_to_res::Vector{Future} =
        isnothing(filtered_to) ? Future[] : Future[convert(Future, filtered_to)]
    scaled_by_same_as_res::Vector{Future} =
        isnothing(scaled_by_same_as) ? Future[] : Future[convert(Future, scaled_by_same_as)]

    # Create PTs for each axis that can be used to block along
    Blocked(f, along_res, balanced_res, filtered_from_res, filtered_to_res, scaled_by_same_as_res)
end

# NOTE: A reason to use Grouped for element-wise computation (with no
# filtering) is to allow for the input to be re-balanced. If you just use
# Any then there wouldn't be any way to re-balance right before the
# computation. Grouped allows the input to have either balanced=true or
# balanced=false and if balanced=true is chosen then a cast may be applied.

const FutureByOptionalKey{K} = Dict{Future,Vector{K}}

function get_factor(
    factor::Float64,
    f_sample::T,
    ff_sample::U,
    fkey::K,
    key::K,
)::Float64 where {T,U,K}

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
    min_filtered_to = sample_min(f_sample, key)
    max_filtered_to = sample_max(f_sample, key)
    # divisions_filtered_from = sample(ff, :statistics, key, :divisions)
    ff_percentile = sample_percentile(ff_sample, fkey, min_filtered_to, max_filtered_to)::Float64
    ffactor::Float64 = 1 / ff_percentile
    max(factor, ffactor)
end

function make_grouped_pt(
    f::Future,
    f_sample::T,
    key::K,
    b::Bool,
    rev::Bool,
    rev_is_nothing::Bool,
    filtered_from::FutureByOptionalKey{K},
    filtered_to::FutureByOptionalKey{K},
    scaled_by_same_as::Vector{Future},
) where {T,K}
    new_pt = Grouped()
    parameters::Dict{String,Any} = new_pt.parameters
    parameters["key"] = key
    parameters["balanced"] = b
    constraints = new_pt.constraints

    # Create `ScaleBy` constraint and also compute `divisions` and
    # `AtMost` constraint if balanced
    if b
        # Set divisions
        # TODO: Change this if `divisions` is not a `Vector{Tuple{Any,Any}}`
        parameters["divisions"] = to_jl_value(sample_divisions(f_sample, key))
        max_ngroups = sample_max_ngroups(f_sample, key)::Int64

        # Set flag for reversing the order of the groups
        if !rev_is_nothing
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
            from::Vector{Future} = Base.collect(keys(filtered_from))
            for (ff, fby) in filtered_from
                fkey::K = isempty(fby) ? key : fby[1]
                # We have ::T here but not in the `ft` case below
                # because of getindex (which is the only case where we
                # call Grouped constructor and have filtered_to being an array while the
                # main future is a data frame)
                ff_sample::T = sample(ff, fkey)
                factor = get_factor(
                    factor,
                    f_sample,
                    ff_sample,
                    key,
                    fkey
                )
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
            to::Vector{Future} = Base.collect(keys(filtered_to))
            for (ft, fby) in filtered_to
                fkey::K = isempty(fby) ? key : fby[1]
                ft_sample = sample(ft, fkey)
                # Compute the amount to scale memory usage by based on data skew
                factor = get_factor(
                    factor,
                    ft_sample,
                    f_sample, 
                    key,
                    fkey
                )
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

    new_pt
end

const NONE = PartitionType("key" => nothing, "balanced" => false, f -> AtMost(0, f))

function make_grouped_pts(
    f::Future,
    # This parameter is passed in just so that we can have type stability in
    # `f_sample` local variable
    f_s::T,
    by::Vector{K},
    balanced::Vector{Bool},
    rev::Bool,
    rev_isnothing::Bool,
    filtered_from::FutureByOptionalKey{K},
    filtered_to::FutureByOptionalKey{K},
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType} where {T,K}
    # Create PTs for each key that can be used to group by
    pts::Vector{PartitionType} = []
    for key in by
        f_sample::T = sample(f, key)
        @time begin
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced
            push!(pts, make_grouped_pt(f, f_sample, key, b, rev, rev_isnothing, filtered_from, filtered_to, scaled_by_same_as))
        end
        println("Finished Grouped for a key")
        end
    end

    # If there are no PTs, ensure that we at least have one impossible PT. This
    # PT doesn't need to have divisions and can be unbalanced but the important
    # thing is that we assign an AtMost-zero constraint which will prevent a PA
    # containing this from being used. This is important because we can't group
    # data on keys that don't belong to it.
    if isempty(pts)
        push!(pts, deepcopy(NONE))
    end

    println("Finished Grouped")

    pts
end

function Grouped(
    f::Future,
    f_s::Sample,
    f_sample::U,
    f_keys::Base.Vector{K},
    # Parameters for splitting into groups
    by::Union{Nothing,Colon,T,Vector{T}},
    balanced::Base.Vector{Bool},
    rev::Union{Nothing,Bool},
    # Options to deal with skew
    filtered_from::Union{Nothing,AbstractFuture,Dict{<:AbstractFuture,T}},
    filtered_to::Union{Nothing,AbstractFuture,Dict{<:AbstractFuture,T}},
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType} where {T,U,K}
    @time begin

    # Prepare `by`
    by_res::Vector{K} = if isnothing(by)
        convert(Vector{K}, f_s.groupingkeys)::Vector{K}
    elseif by isa Colon
        f_keys::Vector{K}
    elseif by isa Vector
        by
    else
        K[by]
    end
    intersect!(by_res, f_keys::Vector{K})
    by_res = by_res[1:min(8,end)]

    filtered_from_res::FutureByOptionalKey{K} =
        if isnothing(filtered_from)
            FutureByOptionalKey{K}()
        elseif filtered_from isa Dict
            FutureByOptionalKey{K}(
                convert(Future, f) => key
                for (f, key) in filtered_from
            )
        else
            FutureByOptionalKey{K}(convert(Future, filtered_from) => K[])
        end
    filtered_to_res::FutureByOptionalKey{K} =
        if isnothing(filtered_to)
            FutureByOptionalKey{K}()
        elseif filtered_to isa Dict
            FutureByOptionalKey{K}(
                convert(Future, f) => key
                for (f, key) in filtered_to
            )
        else
            FutureByOptionalKey{K}(convert(Future, filtered_to) => K[])
        end
    
    println("Finished general Grouped")
    end

    make_grouped_pts(
        f,
        f_sample,
        by_res,
        balanced,
        isnothing(rev) ? false : rev,
        isnothing(rev),
        filtered_from_res,
        filtered_to_res,
        scaled_by_same_as,
    )
end

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
    f_res::Future = convert(Future, f)
    f_s::Sample = get_location(f_res).sample
    f_sample = f_s.value
    f_keys::Base.Vector{String} = sample_keys(f_sample)
    balanced_res::Base.Vector{Bool} = isnothing(balanced) ? Bool[true, false] : Bool[balanced]
    scaled_by_same_as_res::Vector{Future} =
        isnothing(scaled_by_same_as) ? Future[] : Future[convert(Future, scaled_by_same_as)]
    Grouped(
        f_res,
        f_s,
        f_sample,
        f_keys,
        by,
        balanced_res,
        rev,
        filtered_from,
        filtered_to,
        scaled_by_same_as_res
    )
end
