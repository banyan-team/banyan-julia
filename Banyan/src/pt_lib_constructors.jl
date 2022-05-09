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
BlockedAlong()::PartitionType = deepcopy(BLOCKED)
BlockedAlong(along::Int64)::PartitionType =
    PartitionType(
        "name" => "Distributing",
        "distribution" => "blocked",
        "key" => along,
    )
BlockedAlong(along::Int64, balanced::Bool)::PartitionType =
    PartitionType(
        "name" => "Distributing",
        "distribution" => "blocked",
        "key" => along,
        "balanced" => balanced,
    )
# const GROUPED = PartitionType("name" => "Distributing", "distribution" => "grouped")
# Grouped()::PartitionType = deepcopy(GROUPED)
GroupedBy(key) = PartitionType("name" => "Distributing", "distribution" => "grouped", "key" => key)
GroupedBy(key, balanced::Bool) = PartitionType("name" => "Distributing", "distribution" => "grouped", "key" => key, "balanced" => balanced)
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

function _distributed(
    samples_for_grouping::SampleForGrouping{T,K},
    balanced_res::Vector{Bool},
    filtered_relative_to_res::Vector{SampleForGrouping{TF,KF}},
    filtered_from_res::Vector{Future},
    filtered_to_res::Vector{Future},
    filtered_from::Bool,
    scaled_by_same_as_res::Vector{Future},
    rev::Bool,
    rev_is_nothing::Bool,
)::Vector{PartitionType} where {T,K,TF,KF}
    @time begin
    blocked_pts::Vector{PartitionType} = 
        make_blocked_pts(samples_for_grouping.future, samples_for_grouping.axes, balanced_res, filtered_from_res, filtered_to_res, scaled_by_same_as_res)
    println("Time to call make_blocked_pts from Distributed")
    end
    @time begin
    grouped_pts::Vector{PartitionType} =
        make_grouped_pts(
            samples_for_grouping,
            balanced_res,
            rev,
            rev_is_nothing,
            filtered_relative_to_res,
            filtered_from,
            scaled_by_same_as_res,
        )
    println("Time to call make_grouped_pts from Distributed")
    end
    @time begin
    res = vcat(blocked_pts, grouped_pts)
    println("Concatenating lists PTs")
    end
    res
end

function Distributed(
    samples_for_grouping::SampleForGrouping{T,K};
    # Parameters for splitting into groups
    balanced::Union{Nothing,Bool} = nothing,
    rev::Union{Nothing,Bool} = nothing,
    # Options to deal with skew
    filtered_relative_to::Union{SampleForGrouping{TF,KF},Vector{SampleForGrouping{TF,KF}}} = SampleForGrouping{Nothing,Int64}[],
    filtered_from::Bool = false,
    scaled_by_same_as::Union{Future,Nothing} = nothing,
)::Vector{PartitionType} where {T,TF,K,KF}
    # @nospecialize
    # filtered_relative_to_futures::Vector{Future} = !isnothing(filtered_relative_to) ? Future[filtered_relative_to.future] : Future[]
    @time begin
    balanced_res::Vector{Bool} = isnothing(balanced) ? Bool[true, false] : Bool[balanced]
    filtered_relative_to_res::Vector{SampleForGrouping{TF,KF}} = filtered_relative_to isa Vector ? filtered_relative_to : SampleForGrouping{TF,KF}[filtered_relative_to]
    filtered_from_res::Vector{Future} = (filtered_from && !isempty(filtered_relative_to_res)) ? Future[filtered_relative_to_res[1].future] : Future[]
    filtered_to_res::Vector{Future} = (!filtered_from && !isempty(filtered_relative_to_res)) ? Future[filtered_relative_to_res[1].future] : Future[]
    scaled_by_same_as_res::Vector{Future} = (!isnothing(scaled_by_same_as)) ? Future[scaled_by_same_as] : Future[]
    println("Time to prepare for calling _distributed")
    end
    @time begin
    res = _distributed(
        samples_for_grouping,
        balanced_res,
        filtered_relative_to_res,
        filtered_from_res,
        filtered_to_res,
        filtered_from,
        scaled_by_same_as_res,
        isnothing(rev) ? false : rev,
        isnothing(rev)
    )
    println("Time to call _distributed")
    end
    res
end

function get_factor_for_blocked(f_s::Sample, filtered::Vector{Future}, filtered_from::Bool)::Float64
    factor::Float64 = -1
    for ff in filtered
        ff_factor::Float64 = get_location(ff).sample.memory_usage / f_s.memory_usage
        ff_factor_relative::Float64 = filtered_from ? ff_factor : 1/ff_factor
        factor = max(factor, ff_factor_relative)
    end
    factor != -1 || error("Factor of scaling should not be -1")
    factor
end

function make_blocked_pt(
    f::Future,
    axis::Int64,
    filtered_from::Vector{Future},
    filtered_to::Vector{Future},
    scaled_by_same_as::Vector{Future},
)::PartitionType
    # Initialize parameters
    new_pt = BlockedAlong(axis, false)
    constraints = new_pt.constraints.constraints

    # Create `ScaleBy` constraints
    filtered_relative_to::Vector{Future} = !isempty(filtered_from) ? filtered_from : filtered_to
    factor::Float64 = if !isempty(filtered_relative_to)
        # If 100 elements get filtered to 20 elements and the
        # original data was block-partitioned in a balanced
        # way, the result may all be on one partition in the
        # msot extreme case (not balanced at all) and so we
        # should adjust the memory usage of the result by
        # multiplying it by the size of the original / the size
        # of the result (100 / 20 = 5).
        @time begin
        res = get_factor_for_blocked(get_location(f).sample, filtered_relative_to, !isempty(filtered_from))
        println("Time for get_factor_for_blocked in make_blocked_pt")
        end
        res

        # The factor will be infinite or NaN if either what we are
        # filtering from or filtering to has an empty sample. In
        # that case, it wouldn't make sense to have a `ScaleBy`
        # constraint. A value with an empty sample must either be
        # replicated (if it is from an empty dataset) or
        # grouped/blocked but not balanced (since if it were
        # balanced, we might try to use its divisions - which would
        # be empty - for other PTs and think that they to are balanced).
    elseif !isempty(scaled_by_same_as)
        1.0
    else
        NaN
    end

    if factor != Inf && factor != NaN
        push!(
            constraints,
            Scale(
                f,
                by=factor,
                relative_to = if !isempty(filtered_relative_to)
                    filtered_relative_to
                else
                    scaled_by_same_as
                end
            )
        )
    end
    
    new_pt
end

function make_blocked_pts(
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
            if b
                new_pt = BlockedAlong(axis, true)
                push!(new_pt.constraints.constraints, noscale(f))
                push!(pts,new_pt)
            else
                push!(pts, make_blocked_pt(f, axis, filtered_from, filtered_to, scaled_by_same_as))
            end
        end
    end

    # Return the resulting PT union that can then be passed into a call to `pt`
    # which would in turn result in a PA union
    pts
end

function Blocked(
    f::Future;
    along::Vector{Int64} = sample_axes(sample(f)),
    balanced::Union{Nothing,Bool} = nothing,
    filtered_from::Union{Nothing,Future} = nothing,
    filtered_to::Union{Nothing,Future} = nothing,
    scaled_by_same_as::Union{Nothing,Future} = nothing,
)::Vector{PartitionType}
    # f = convert(Future, f)::Future

    # Prepare `along`
    # along_res::Vector{Int64} = along
    # TODO: Ensure that axes returns [1] for DataFrame and axes for Array
    # while keys returns keys for DataFrame and axes for Array
    # TODO: Maybe assert that along isa Vector{String} or Vector{Symbol}

    balanced_res::Vector{Bool} = isnothing(balanced) ? Bool[true, false] : Bool[balanced]

    filtered_from_res::Vector{Future} =
        isnothing(filtered_from) ? Future[] : Future[filtered_from] # Future[convert(Future, filtered_from)]
    filtered_to_res::Vector{Future} =
        isnothing(filtered_to) ? Future[] : Future[filtered_to] # Future[convert(Future, filtered_to)]
    scaled_by_same_as_res::Vector{Future} =
        isnothing(scaled_by_same_as) ? Future[] : Future[scaled_by_same_as] # Future[convert(Future, scaled_by_same_as)]

    # Create PTs for each axis that can be used to block along
    make_blocked_pts(f, along, balanced_res, filtered_from_res, filtered_to_res, scaled_by_same_as_res)
end
   
# BlockedOnAny(
#     f::Future;
#     balanced::Union{Nothing,Bool} = nothing,
#     filtered_from::Union{Nothing,Future} = nothing,
#     filtered_to::Union{Nothing,Future} = nothing,
#     scaled_by_same_as::Union{Future,Nothing} = nothing,
# )::Vector{PartitionType} =
#     Blocked(
#         f, along=sample_axes(sample(f))::Vector{Int64},
#         balanced=balanced,
#         filtered_from=filtered_from, filtered_to=filtered_to,
#         scaled_by_same_as=scaled_by_same_as
#     )

# NOTE: A reason to use Grouped for element-wise computation (with no
# filtering) is to allow for the input to be re-balanced. If you just use
# Any then there wouldn't be any way to re-balance right before the
# computation. Grouped allows the input to have either balanced=true or
# balanced=false and if balanced=true is chosen then a cast may be applied.

const FutureByOptionalKey{K} = Dict{Future,Vector{K}}

function _get_factor(
    factor::Float64,
    f::Tuple{T,K},
    frt::Tuple{TF,KF}
)::Float64 where {T,TF,K,KF}
    f_sample, key = f
    frt_sample, frtkey = frt
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
    @time begin
    min_frt = sample_min(f_sample, key)
    println("Time for calling sample_min")
    end
    @time begin
    max_frt = sample_max(f_sample, key)
    println("Time for calling sample_max")
    end
    # divisions_filtered_from = sample(ff, :statistics, key, :divisions)
    @time begin
    frt_percentile = sample_percentile(frt_sample, frtkey, min_frt, max_frt)::Float64
    println("Time for calling sample_percentile")
    end
    frtfactor::Float64 = 1 / frt_percentile
    max(factor, frtfactor)
end

function get_factor(
    factor::Float64,
    f_sample::T,
    key::K,
    frt_samples::SampleForGrouping{TF,K},
    filtered_from::Bool
)::Float64 where {T,TF,K}
    # We want to find the factor of scaling to account for data skew when
    # filtering from/to some other future. So 
    f_pair = (f_sample, key)
    frt_sample = frt_samples.sample
    frt_pair = if key in frt_samples.keys
        (frt_sample, key)
    else
        (frt_sample, frt_samples.keys[1])
    end
    @time begin
    res = _get_factor(
        factor,
        filtered_from ? f_pair : frt_pair,
        filtered_from ? frt_pair : f_pair
    )
    println("Time for calling _get_factor")
    end
    res
end

function get_factor(
    factor::Float64,
    f_sample::T,
    key::K,
    frt_samples::SampleForGrouping{TF,KF},
    filtered_from::Bool
)::Float64 where {T,TF,K,KF}
    frt_sample = frt_samples.sample
    fkey = frt_samples.keys[1]
    f_pair = (f_sample, key)
    frt_pair = (frt_sample, fkey)
    @time begin
    res = _get_factor(
        factor,
        filtered_from ? f_pair : frt_pair,
        filtered_from ? frt_pair : f_pair
    )
    println("Time for calling _get_factor")
    end
    res
end

function make_grouped_balanced_pt(
    samples_for_grouping::SampleForGrouping{T,K},
    key::K,
    rev::Bool,
    rev_is_nothing::Bool,
)::PartitionType where {T,K}
    new_pt = GroupedBy(key, true)
    parameters::Dict{String,Any} = new_pt.parameters
    constraints = new_pt.constraints.constraints

    f::Future = samples_for_grouping.future
    f_sample = samples_for_grouping.sample
    
    # Set divisions
    # TODO: Change this if `divisions` is not a `Vector{Tuple{Any,Any}}`
    @time begin
    sampled_divisions = sample_divisions(f_sample, key)
    println("Time for sample_divisions( in make_grouped_balanced_pt")
    end
    @time begin
    parameters["divisions"] = to_jl_value(sampled_divisions)
    println("Time for to_jl_value( in make_grouped_balanced_pt")
    end
    @time begin
    max_ngroups = sample_max_ngroups(f_sample, key)::Int64
    println("Time for sample_max_ngroups")
    end

    # Set flag for reversing the order of the groups
    if !rev_is_nothing
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
    @time begin
    push!(constraints, AtMost(max_ngroups, f))
    println("Time for AtMost")
    end
    @time begin
    push!(constraints, Scale(f, by=1.0))
    println("Time for Scale")
    end

    # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
    # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
    # that the constraint can be satisfied for this PT to be used

    new_pt
end

function make_grouped_filtered_pt(
    samples_for_grouping::SampleForGrouping{T,K},
    key::K,
    filtered_relative_to::Vector{SampleForGrouping{TF,KF}},
    filtered_from::Bool,
)::PartitionType where {T,TF,K,KF}
    new_pt = GroupedBy(key, false)

    # Create `ScaleBy` constraint and also compute `divisions` and
    # `AtMost` constraint if balanced

    # TODO: Support joins
    factor::Float64 = 0
    f_sample = samples_for_grouping.sample
    relative_to::Vector{Future} = Future[]
    for frt in filtered_relative_to
        push!(relative_to, frt.future)
        @time begin
        factor = get_factor(
            factor,
            f_sample,
            key,
            frt,
            filtered_from
        )
        println("Finished running get_factor")
        end
    end

    # If the sample of what we are filtering into is empty, the
    # factor will be infinity. In that case, we shouldn't be
    # creating a ScaleBy constraint.
    f::Future = samples_for_grouping.future
    if factor != Inf
        push!(new_pt.constraints.constraints, Scale(f, by=factor, relative_to=relative_to))
    end

    new_pt
end

function make_grouped_pt(
    f::Future,
    key::K,
    scaled_by_same_as::Vector{Future},
) where {K}
    new_pt = GroupedBy(key, false)
    push!(new_pt.constraints.constraints, Scale(f, by=1.0, relative_to=scaled_by_same_as))
    new_pt
end

const NONE = PartitionType("key" => nothing, "balanced" => false, f -> AtMost(0, f))

function make_grouped_pts(
    # This parameter is passed in just so that we can have type stability in
    # `f_sample` local variable
    samples_for_grouping::SampleForGrouping{T,K},
    balanced::Vector{Bool},
    rev::Bool,
    rev_isnothing::Bool,
    filtered_relative_to::Vector{SampleForGrouping{TF,KF}},
    filtered_from::Bool,
    scaled_by_same_as::Vector{Future},
)::Vector{PartitionType} where {K,T,KF,TF}
    @time begin
    # Create PTs for each key that can be used to group by
    pts::Vector{PartitionType} = []
    for key in samples_for_grouping.keys
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced
            if b
                @time begin
                push!(pts, make_grouped_balanced_pt(samples_for_grouping, key, rev, rev_isnothing))
                println("Finished calling make_grouped_balanced_pt")
                end
            elseif !isempty(filtered_relative_to)
                @time begin
                push!(pts, make_grouped_filtered_pt(samples_for_grouping, key, filtered_relative_to, filtered_from))
                println("Finished calling make_grouped_filtered_pt")
                end
            elseif !isempty(scaled_by_same_as)
                @time begin
                push!(pts, make_grouped_pt(samples_for_grouping.future, key, scaled_by_same_as))
                println("Finished calling make_grouped_pt")
                end
            end
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

    println("Finished inside of make_grouped_pts")
    end

    pts
end

# function convert_to_future_by_optional(::Type{K}, filtered_from::Nothing)::FutureByOptionalKey{K} where {K} FutureByOptionalKey{K}() end
# function convert_to_future_by_optional(::Type{K}, filtered_from::Future)::FutureByOptionalKey{K} where {K}
#     FutureByOptionalKey{K}(filtered_from => K[])
# end
# function convert_to_future_by_optional(::Type{K}, filtered_from::Dict{Future,K})
#     filtered_from_res::FutureByOptionalKey{K} = FutureByOptionalKey{K}()
#     for (f::Future, key::K) in filtered_from_res
#         filtered_from_res[f] = key
#     end
#     filtered_from_res
# end

# function Grouped(
#     f::AbstractFuture,
#     samples_for_grouping::Vector{Tuple{K,T}},
#     # Parameters for splitting into groups
#     balanced::Union{Nothing,Bool},
#     rev::Union{Nothing,Bool},
#     # Options to deal with skew
#     filtered_from::Union{Nothing,Future,Dict{Future,K}},
#     filtered_to::Union{Nothing,Future,Dict{Future,K}},
#     scaled_by_same_as::Union{Nothing,Vector{Future}},
# )::Vector{PartitionType} where {T,K}
#     @time begin

#     filtered_from_res::FutureByOptionalKey{K} = convert_to_future_by_optional(K, filtered_from)
#     filtered_to_res::FutureByOptionalKey{K} = convert_to_future_by_optional(K, filtered_to)
    
#     println("Finished general Grouped")
#     end

#     make_grouped_pts(
#         convert(Future, f),
#         samples_for_grouping,
#         isnothing(balanced) ? Bool[true, false] : Bool[balanced],
#         isnothing(rev) ? false : rev,
#         isnothing(rev),
#         filtered_from_res,
#         filtered_to_res,
#         isnothing(scaled_by_same_as) ? Future[] : scaled_by_same_as,
#     )
# end

function Grouped(
    samples_for_grouping::SampleForGrouping{T,K};
    # Parameters for splitting into groups
    balanced::Union{Nothing,Bool} = nothing,
    rev::Union{Nothing,Bool} = nothing,
    # Options to deal with skew
    filtered_relative_to::Union{SampleForGrouping{TF,KF},Vector{SampleForGrouping{TF,KF}}} = SampleForGrouping{Nothing,Int64}[],
    filtered_from::Bool = false,
    scaled_by_same_as::Union{Future,Nothing} = nothing,
)::Vector{PartitionType} where {K,T,KF,TF}
# function Grouped(
#     samples_for_grouping::SampleForGrouping{T,K};
#     # Parameters for splitting into groups
#     balanced::Union{Nothing,Bool} = nothing,
#     rev::Union{Nothing,Bool} = nothing,
#     # Options to deal with skew
#     filtered_relative_to::Union{SampleForGrouping{TF,KF},Vector{SampleForGrouping{TF,KF}}} = SampleForGrouping{Nothing,Int64}[],
#     filtered_from::Bool = false,
#     scaled_by_same_as::Union{Future,Nothing} = nothing,
# )::Vector{PartitionType} where {K,T,KF,TF}
    # @nospecialize
    # Grouped(
    #     f,
    #     samples_for_grouping,
    #     balanced,
    #     rev,
    #     filtered_from,
    #     filtered_to,
    #     scaled_by_same_as
    # )
    # @time begin

    # # filtered_relative_to::FutureByOptionalKey{K} = convert_to_future_by_optional(K, filtered)
    
    # println("Finished convert_to_future_by_optional")
    # end

    @time begin
    # res = make_grouped_pts(
    #     samples_for_grouping,
    #     isnothing(balanced) ? Bool[true, false] : Bool[balanced],
    #     isnothing(rev) ? false : rev,
    #     isnothing(rev),
    #     filtered_relative_to isa Base.Vector ? filtered_relative_to : [filtered_relative_to],
    #     filtered_from,
    #     isnothing(scaled_by_same_as) ? Future[] : Future[scaled_by_same_as],
    # )
    res = make_grouped_pts(
        samples_for_grouping,
        isnothing(balanced) ? Bool[true, false] : Bool[balanced],
        isnothing(rev) ? false : rev,
        isnothing(rev),
        filtered_relative_to isa Base.Vector ? filtered_relative_to : SampleForGrouping{TF,KF}[filtered_relative_to],
        filtered_from,
        isnothing(scaled_by_same_as) ? Future[] : Future[scaled_by_same_as],
    )
    println("Finished calling make_grouped_pts")
    end
    res
end

# sample_for_grouping called from annotation code on the main future as well as all things being filtered to/from
# T, K, TF, KF
# make_grouped_pts should call make_grouped_pt on each grouping key from the sample
# make_grouped_pt shuld have a special case for where the key types are the same - try to use the right sample; otherwise just use any sample
# make_grouped_pt should be broken into helper functions that don't rely on T and maybe just rely on K

# Three use-cases of Grouped / filtered_from/filtered_to:
# - pts_for_filtering for getindex - sample of filtered_to can be different and any key 
# - Grouped with filtered_to for innerjoin - key is specified for each future