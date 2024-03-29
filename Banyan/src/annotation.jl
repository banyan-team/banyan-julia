###############################
# Global state for annotation #
###############################

# TODO: Support multi-threaded usage by storing a global array with an instance
# of this annotation state for each thread
curr_delayed_task = DelayedTask()

function set_task(t::DelayedTask)
    global curr_delayed_task
    curr_delayed_task = t
end

function get_task()::DelayedTask
    global curr_delayed_task
    curr_delayed_task
end

function finish_task()
    set_task(DelayedTask())
end

function get_pa()::PartitionAnnotation
    get_task().pa_union[end]
end

function finish_pa()
    curr_delayed_task = get_task()
    push!(curr_delayed_task.pa_union, PartitionAnnotation())
end

#################################################
# Setting up sample properties in beforecompute #
#################################################

# These functions are necessary because when we construct PT's to assign to
# futures for annotated code (especially code in BDF.jl), we rely on statistics
# of the samples of the futures to construct the PTs. But computing statistics
# is expensive and we only want to compute it for the keys/columns that are
# actually used for grouping/sorting/etc. So we propagate this through the
# `:groupingkeys` sample property of different futures (each future has a
# set of sample properties).
# 
# These `keep_*` functions specify how `statistics` and `groupingkeys` sample
# properties should propagate. These functions are called inside of a
# `partitioned_using` so that they are called in a forward pass through all the
# tasks (code regions) detected and also in a backward pass to properly
# propagate the `groupingkeys` sample property. For example, a groupby may only
# happen in the very last task indicating that a particular column can be in
# the `groupingkeys` (the column used for the groupby) but we need to specify
# that that column could have been used for grouping the data throughout (if
# all tasks are amenable like that).
# 
# Of course, columns can be dropped or
# renamed and so that makes things a bit challenging in properly propagating
# information about sample properties and that's why we have a lot of these
# keep_* functions.

function _get_new_p_groupingkeys!(p::Future, p_s::Sample, participants::Base.Vector{Future}, drifted::Bool, p_keys::Base.Vector{K}, keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}) where {K}
    new_p_groupingkeys = K[]
    for op in participants
        op_groupingkeys::Vector{K} = get_location(op).sample.groupingkeys
        for op_groupingkey in op_groupingkeys
            if op_groupingkey in p_keys && !(op_groupingkey in new_p_groupingkeys)
                push!(new_p_groupingkeys, op_groupingkey)

                # Only allow keys that are actually in the keys of the participants
                # TODO: Maybe replace this with a function that doesn't require calling
                # a potentially expensive function to iterate through _all_ columns.
                if !drifted
                    push!(keeping_same_statistics, (p, op_groupingkey, op, op_groupingkey))
                end
            end
        end
    end
    p_s.groupingkeys = new_p_groupingkeys
end

function get_new_p_groupingkeys!(p::Future, p_s::Sample, participants::Base.Vector{Future}, drifted::Bool, p_sample::T, keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}) where {T,K}
    _get_new_p_groupingkeys!(p, p_s, participants, drifted, sample_keys(p_sample), keeping_same_statistics)
end

function keep_all_sample_keys(participants::Base.Vector{Future}, drifted::Bool, keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}) where {K}
    if isempty(participants)
        return
    end

    # This can be use on an input and output that share the same keys that they
    # can be grouped by.

    # Take the union of discovered grouping keys. Grouping keys are discovered
    # through calls to `keep_keys` in functions where we know that we need
    # to allow grouping by certain keys such as sorting and join functions.
    for p in participants
        p_s::Sample = get_location(p).sample
        p_sample = p_s.value
        get_new_p_groupingkeys!(p, p_s, participants, drifted, p_sample, keeping_same_statistics)
    end
end

function apply_keeping_same_statistics(keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}) where {K}
    for same in keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}
        keep_same_statistics(same[1], same[2], same[3], same[4])
    end
end

function keep_all_sample_keys_renamed(
    o::Tuple{Future,Sample,Vector{K}},
    n::Tuple{Future,Sample,Vector{K}},
    keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}
) where {K}
    old, old_sample, old_keys = o
    new, new_sample, new_keys = n
    old_groupingkeys::Vector{K} = old_sample.groupingkeys
    old_groupingkeys_changed = false
    new_groupingkeys::Vector{K} = new_sample.groupingkeys
    new_groupingkeys_changed = false
    length(old_keys) == length(new_keys) || error("Expected renaming operation to not change the number of keys/axes/columns of this data")
    for i = 1:length(old_keys)
        ok::K = old_keys[i]
        nk::K = new_keys[i]
        if ok in old_groupingkeys
            push!(new_groupingkeys, nk)
            push!(keeping_same_statistics, (new, nk, old, ok))
            new_groupingkeys_changed = true
        end
        if nk in new_groupingkeys
            push!(old_groupingkeys, ok)
            push!(keeping_same_statistics, (old, ok, new, nk))
            old_groupingkeys_changed = true
        end
    end
    if new_groupingkeys_changed
        new_sample.groupingkeys = new_groupingkeys
    end
    if old_groupingkeys_changed
        old_sample.groupingkeys = old_groupingkeys
    end
end

function keep_all_sample_keys_renamed(
    o::Tuple{Future,Sample,T},
    n::Tuple{Future,Sample,T},
    ::Type{K},
    keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}
) where {K,T}
    _keep_all_sample_keys_renamed(
        (o[1], o[2], sample_keys(o[3])),
        (n[1], n[2], sample_keys(n[3])),
        keeping_same_statistics
    )
end

keep_all_sample_keys_renamed(
    old::Future,
    new::Future,
    ::Type{K},
    keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}
) where {K} = begin
    old_s = get_location(old).sample
    new_s = get_location(old).sample
    keep_all_sample_keys_renamed((old, old_s, old_s.value), (new, new_s, new_s.value), K, keeping_same_statistics)
end

function keep_sample_keys_named(
    participants::Vector{Tuple{Future,Vector{K}}},
    drifted::Bool,
    keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}
) where {K}
    # `participants` maps from futures to lists of key names such that all
    # participating futures have the same sample properties for the keys at
    # same indices in those lists
    first_participant_pair = first(participants)
    first_participant::Future = first_participant_pair[1]
    first_keys::Vector{K} = first_participant_pair[2]
    nkeys = length(first_keys)
    for i = 1:nkeys
        # Copy over allowed grouping keys
        for (p::Future, keys::Vector{K}) in participants
            p_key::K = keys[i]
            p_sample = get_location(p).sample
            p_sample_groupingkeys::Vector{K} = p_sample.groupingkeys
            if !(p_key in p_sample_groupingkeys)
                push!(p_sample_groupingkeys, p_key)
            end
            p_sample.groupingkeys = p_sample_groupingkeys

            # Copy over statistics if they haven't changed
            if !drifted
                for gk::K in p_sample_groupingkeys
                    push!(keeping_same_statistics, (p, gk, first_participant, first_keys[i]))
                end
            end
        end
    end
end

# NOTE: For operations where join rate and data skew might be different, it is
# assumed that they are the same. For example, column vectors from the result
# of a join should have the same sample rate and the same data skew.

keep_sample_keys(keys::Vector{K}, participants::Vector{Future}, drifted::Bool, keeping_same_statistics::Vector{Tuple{Future,K,Future,K}}) where K =
    keep_sample_keys_named(Tuple{Future,Vector{K}}[(p, keys) for p in participants], drifted, keeping_same_statistics)

# This is useful for workloads that involve joins where the sample rate is
# diminished quadratically for each joinv
# TODO: Make _this_ more accurate for `innerjoin``
function keep_sample_rate(fut::Future, relative_to::Future)
    fut_sample::Sample = get_location(fut).sample
    relative_to_sample::Sample = get_location(relative_to).sample
    fut_sample.rate = relative_to_sample.rate
end
function keep_sample_rate(fut::Future, relative_to::Vector{Future})
    rate_prod::Int64 = 1
    for r in relative_to
        rate_prod = rate_prod * get_location(r).sample.rate
    end
    get_location(fut).sample.rate = rate_prod
end

###############################
# Using samples to assign PTs #
###############################

function get_inputs_and_outputs(
    curr_delayed_task_mutation::IdDict{Future,Future},
    curr_delayed_task_scaled::Vector{Future},
    grouping_needed::Bool,
    grouped::Vector{Future}
)::Tuple{Vector{Future},Vector{Future},Vector{Future},Vector{Future}}
    output_value_ids = ValueId[]
    for ff in values(curr_delayed_task_mutation)
        push!(output_value_ids, ff.value_id)
    end
    outputs::Vector{Future} = Future[]
    inputs::Vector{Future} = Future[]
    for f in curr_delayed_task_scaled
        if f.value_id in output_value_ids
            push!(outputs, f)
        else
            push!(inputs, f)
        end
    end

    outputs_grouped::Vector{Future} = Future[]
    inputs_grouped::Vector{Future} = Future[]
    if grouping_needed
        for f in grouped
            if f.value_id in output_value_ids
                push!(outputs_grouped, f)
            else
                push!(inputs_grouped, f)
            end
        end
    end

    outputs, inputs, outputs_grouped, inputs_grouped
end

function apply_partitioned_using_func_for_sample_rates(inputs::Vector{Future}, keep_same_sample_rate::Bool, outputs::Vector{Future})
    if !isempty(inputs)
        if keep_same_sample_rate
            for r in outputs
                keep_sample_rate(r, inputs[1])
            end
            for i in 1:(length(inputs)-1)
                this_sample_rate = get_location(inputs[i]).sample.rate
                other_sample_rate = get_location(inputs[i+1]).sample.rate
                if this_sample_rate != other_sample_rate
                    @warn "Two inputs have different sample rates ($this_sample_rate, $other_sample_rate)"
                end
            end
        else
            for r in outputs
                keep_sample_rate(r, inputs)
            end
        end
    end
end


function apply_partitioned_using_func(f::PartitionedUsingFunc{K}) where {K}
    keep_same_sample_rate::Bool = f.keep_same_sample_rate
    # Keys (not relevant if you never use grouped partitioning).
    grouped::Vector{Future} = f.grouped
    keep_same_keys::Bool = f.keep_same_keys
    keys::Vector{K} = f.keys
    keys_by_future::Vector{Tuple{Future,Vector{K}}} = f.keys_by_future
    renamed::Bool = f.renamed
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted::Bool = f.drifted

    # Categorize the futures. We determine whether a future is an input by
    # checking if it's not in the values of the task's mutation. All
    # outputs would require mutation for the future to be an output. And
    # that would have been marked by a call to `mutated` that is made in the
    # `Future` constructor.
    grouping_needed = keep_same_keys || !isempty(keys) || !isempty(keys_by_future) || renamed
    curr_delayed_task::DelayedTask = get_task()
    curr_delayed_task_mutation::IdDict{Future,Future} = curr_delayed_task.mutation
    curr_delayed_task_scaled::Vector{Future} = curr_delayed_task.scaled
    outputs::Vector{Future}, inputs::Vector{Future}, outputs_grouped::Vector{Future}, inputs_grouped::Vector{Future} =
        get_inputs_and_outputs(curr_delayed_task_mutation, curr_delayed_task_scaled, grouping_needed, grouped)

    # Propagate information about keys that can be used for grouping
    if grouping_needed
        keeping_same_statistics::Vector{Tuple{Future,K,Future,K}} = Tuple{Future,K,Future,K}[]
        if keep_same_keys
            if renamed
                if length(inputs_grouped) != 1 || length(outputs_grouped) != 1
                    error("Only 1 argument can be renamed to 1 result at once")
                end
                keep_all_sample_keys_renamed(inputs_grouped[1], outputs_grouped[1], K, keeping_same_statistics)
            else
                keep_all_sample_keys(grouped, drifted, keeping_same_statistics)
            end
        end
        if !isempty(keys)
            keep_sample_keys(keys, grouped, drifted, keeping_same_statistics)
        end
        if !isempty(keys_by_future)
            keep_sample_keys_named(keys_by_future, drifted, keeping_same_statistics)
        end
        if !isempty(keeping_same_statistics)
            apply_keeping_same_statistics(keeping_same_statistics)
        end
    end

    # Propgate sample rates
    apply_partitioned_using_func_for_sample_rates(inputs, keep_same_sample_rate, outputs)

    # Store the important inputs and outputs for scaling memory usage
    curr_delayed_task.inputs = inputs
    curr_delayed_task.outputs = outputs
end

function _partitioned_with(
    @nospecialize(handler::Function),
    futures::Vector{Future},
    # Memory usage, sampling
    # `scaled` is the set of futures with memory usage that can potentially be
    # scaled to larger sizes if the amount of data at a location changes.
    # Non-scaled data has fixed memory usage regardless of its sample rate.
    scaled::Vector{Future},
    keep_same_sample_rate::Bool,
    memory_usage::Vector{PartitioningConstraint},
    additional_memory_usage::Vector{PartitioningConstraint},
    # Keys (not relevant if you never use grouped partitioning).
    grouped::Vector{Future},
    keep_same_keys::Bool,
    keys::Vector{K},
    keys_by_future::Vector{Tuple{Future,Vector{K}}},
    renamed::Bool,
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted::Bool,
    # For generating import statements
    modules::Vector{String}
) where {K}
    curr_delayed_task::DelayedTask = get_task()

    if !isempty(modules)
        partitioned_using_modules(modules)
    end

    curr_delayed_task.futures = futures
    curr_delayed_task.scaled = scaled
    curr_delayed_task.partitioned_with_func = handler
    curr_delayed_task.keep_same_sample_rate = keep_same_sample_rate
    curr_delayed_task.memory_usage_constraints = memory_usage
    curr_delayed_task.additional_memory_usage_constraints = additional_memory_usage

    # TODO: Ensure partitioned_using is able to capture updates to the task when mutating

    curr_delayed_task.partitioned_using_func =
        PartitionedUsingFunc{K}(
            keep_same_sample_rate,
            grouped,
            keep_same_keys,
            keys,
            keys_by_future,
            renamed,
            drifted,
            false
        )
end

function partitioned_with(
    @nospecialize(handler::Function),
    futures::Vector{Future};
    # Memory usage, sampling
    # `scaled` is the set of futures with memory usage that can potentially be
    # scaled to larger sizes if the amount of data at a location changes.
    # Non-scaled data has fixed memory usage regardless of its sample rate.
    scaled::Vector{Future} = Future[],
    keep_same_sample_rate::Bool = true,
    memory_usage::Vector{PartitioningConstraint} = PartitioningConstraint[],
    additional_memory_usage::Vector{PartitioningConstraint} = PartitioningConstraint[],
    # Keys (not relevant if you never use grouped partitioning).
    grouped::Vector{Future} = Future[],
    keep_same_keys::Bool = false,
    keys::Union{Vector,Nothing} = nothing,
    keys_by_future::Union{Vector{Tuple{Future,Vector}},Nothing} = nothing,
    renamed::Bool = false,
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted::Bool = false,
    # For generating import statements
    modules::Vector{String} = String[],
    keytype::DataType = Int64,
)
    # scaled
    scaled_res::Vector{Future} = scaled

    # grouped
    grouped_res::Vector{Future} = if isempty(grouped)
        scaled_res
    else
        grouped
    end

    # modules
    modules_res::Vector{String} = modules isa Vector ? modules : [modules]

    # Call `partitioned_with` with restricted concrete types
    K = if !isnothing(keys)
        eltype(keys)
    elseif !isnothing(keys_by_future)
        eltype(first(keys_by_future)[2])
    else
        # This won't be used so we just use something so that the precompiled
        # versions of functions are used.
        keytype
    end
    _partitioned_with(
        handler,
        futures,
        scaled_res,
        keep_same_sample_rate,
        memory_usage,
        additional_memory_usage,
        grouped_res,
        keep_same_keys,
        isnothing(keys) ? K[] : keys,
        isnothing(keys_by_future) ? Tuple{Future,Vector{K}}[] : keys_by_future,
        renamed,
        drifted,
        modules_res,
    )
end

function evaluate_constraint(constraint::PartitioningConstraint, fut::Future)::PartitioningConstraint
    constraint_func::Function = constraint.func
    constraint_func != identity ? constraint_func(fut) : constraint
end

evaluate_constraint_func(fut::Future) = constraint -> evaluate_constraint(constraint, fut)

function pt_partition_type_composition(
    fut::Future,
    ptype::PartitionTypeComposition,
    to_match::Vector{Future},
    on::Vector{String},
    to_cross::Vector{Future}
)

    # Start new PA if this assignment would overwrite one for the current
    # PA. When we start a new PA, we append the old one the PA union for
    # the task being currently constructed.
    pa::PartitionAnnotation = let current_pa = get_pa()
        if haskey(current_pa.partitions.pt_stacks, fut.value_id)
            finish_pa()
            get_pa()
        else
            current_pa
        end
    end

    # Handle constraints that have been delayed till PT assignment
    efunc = evaluate_constraint_func(fut)
    for pty in ptype.pts
        new_constraints::Vector{PartitioningConstraint} = map(efunc, pty.constraints.constraints)
        pty.constraints.constraints = new_constraints
    end

    # Add to PAs information about how partitions are produced
    pa.partitions.pt_stacks[fut.value_id] = ptype

    # Handle `match`, `on` in keyword arguments
    if !isempty(to_match)
        for to_match_with in to_match
            fut_and_to_match_with = Future[fut, to_match_with]
            if !isempty(on)
                for to_match_on in on
                    push!(
                        pa.constraints.constraints,
                        MatchOn(to_match_on, fut_and_to_match_with),
                    )
                end
            else
                push!(pa.constraints.constraints, Match(fut_and_to_match_with))
            end
        end
    end

    if !isempty(to_cross)
        push!(pa.constraints.constraints, Cross(to_cross))
    end
end

function pt_partition_type(ptype::PartitionType, futs::Vector{Future}, match::Vector{Future}, on::Vector{String}, cross::Vector{Future})
    pt_composition::PartitionTypeComposition =
        PartitionTypeComposition(PartitionType[ptype])
    for fut in futs
        pt_partition_type_composition(fut, deepcopy(pt_composition), match, on, cross)
    end
end

function pt_partition_type(ptype::PartitionTypeComposition, futs::Vector{Future}, match::Vector{Future}, on::Vector{String}, cross::Vector{Future})
    for fut in futs
        pt_partition_type_composition(fut, ptype, match, on, cross)
    end
end

function pt_partition_type(ptype::Base.Vector{PartitionType}, futs::Vector{Future}, match::Vector{Future}, on::Vector{String}, cross::Vector{Future})
    for fut in futs
        for i in 1:length(ptype)
            pt_composition = PartitionTypeComposition(ptype[i:i])
            pt_partition_type_composition(fut, pt_composition, match, on, cross)
        end
    end
end

function pt(
    # # args::Union{Future,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    # args...;
    # match = nothing,
    # on = nothing,
    # cross = nothing
    # args::Union{Future,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    args...;
    match::Union{Nothing,Future} = nothing,
    on::Union{String,Vector{String}} = String[],
    cross::Vector{Future} = Future[]
    # args::Union{AbstractFuture,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    # match::Union{Nothing,AbstractFuture} = nothing,
    # on::Union{String,Vector{String}} = String[],
    # cross::Vector{<:AbstractFuture} = Future[]
)
    @nospecialize
    length(args) >= 1 || error("Cannot assign partition type with `pt` unless at least one argument is passed in")

    futs_res::Vector{Future} = Base.collect(args[1:end-1])
    match_res::Vector{Future} = isnothing(match) ? Future[] : Future[match]
    on_res::Vector{String} = isnothing(on) ? String[] : (on isa String ? String[on] : on)
    cross_res::Vector{Future} = isnothing(cross) ? Future[] : cross

    ptype = deepcopy(args[end])
    if length(futs_res) > 0
        pt_partition_type(ptype, futs_res, match_res, on_res, cross_res)
    end
end

# NOTE: `mutated` should not be used inside of `partitioned_with` or
# `partitioned_using`

mutated(f::Future) = mutated(f, f)
mutated(ff::Pair{Future,Future}) = mutated(ff.first, ff.second)

function mutated(old::Future, new::Future)
    # if haskey(curr_delayed_task.value_names, old.value_id)
    #     finish_pa()
    t::DelayedTask = get_task()
    t.mutation[old] = new
end

#################################################
# Macro for wrapping the code region to offload #
#################################################

function apply_mutation(old::Future, new::Future)
    # Apply the mutation by setting the value ID of the old future the
    # value ID of the new one. That way, the user can continue using
    # the old future as if it were mutated but it will be having a
    # different value ID.

    # Swap (1) references in `futures_on_client` if either side of the
    # mutation is on the client
    futures_on_client::Dict{ValueId,Future} = get_session().futures_on_client
    old_on_client = haskey(futures_on_client, old.value_id)
    new_on_client = haskey(futures_on_client, new.value_id)
    if old_on_client && new_on_client
        futures_on_client[new.value_id], futures_on_client[old.value_id] =
            futures_on_client[old.value_id], futures_on_client[new.value_id]
    elseif old_on_client
        futures_on_client[new.value_id] = futures_on_client[old.value_id]
        delete!(futures_on_client, old.value_id)
    elseif new_on_client
        futures_on_client[old.value_id] = futures_on_client[new.value_id]
        delete!(futures_on_client, new.value_id)
    end

    # Swap (2) other fields of the `Future`s and (3) their locations
    session_locations::Dict{ValueId,Location} = get_session().locations
    old.value,
    new.value,
    old.value_id,
    new.value_id,
    old.mutated,
    new.mutated,
    old.stale,
    new.stale,
    old.total_memory_usage,
    new.total_memory_usage,
    session_locations[old.value_id],
    session_locations[new.value_id] =
    new.value,
    old.value,
    new.value_id,
    old.value_id,
    new.mutated,
    old.mutated,
    new.stale,
    old.stale,
    new.total_memory_usage,
    old.total_memory_usage,
    session_locations[new.value_id],
    session_locations[old.value_id]
end

function apply_mutation(mutation::IdDict{Future,Future}, inverted::Bool)
    for (old, new) in mutation
        if old !== new
            if !inverted
                apply_mutation(old, new)
            else
                apply_mutation(new, old)
            end
        end
    end
end

function partitioned_using_modules(m::Vector{String})
    curr_delayed_task = get_task()
    union!(curr_delayed_task.used_modules, m)
end

function finish_partitioned_code_region(splatted_futures::Vector{Future})
    task::DelayedTask = get_task()

    # Update mutated futures
    for fut in splatted_futures
        for m in values(task.mutation)
            if fut.value_id == m.value_id
                fut.stale = true
                fut.mutated = true
                break
            end
        end
    end

    # Apply all delayed source and destination assignment. This will
    # perform any expensive sample collection that may require for example
    # an expensive scan of S3. This will record `RecordLocationRequest`s.
    for splatted_future in splatted_futures
        apply_sourced_or_destined_funcs(splatted_future)
    end

    # Look at mutation, inputs, outputs, and constraints to determine
    # initial/final/additional memory usage and also to issue destroy
    # requests for all mutated values. Maybe also set to nothing and
    # assign new value here for mutation. Also set new future's total
    # memory usage.

    # Get the initial memory usage
    for fut in splatted_futures
        fut_initial_memory_usage::Int64 = if is_total_memory_usage_known(fut)
            fut.total_memory_usage
        else
            tmu::Int64 = try
                get_location(fut).total_memory_usage
            catch e
                if e isa MethodError
                    error("Future with value ID $(fut.value_id) has no initial memory usage even in location with source name $(get_location(fut).src_name)")
                else
                    rethrow()
                end
                -1
            end
            tmu
        end
        task.memory_usage[fut.value_id] = Dict{String,Int64}("initial" => fut_initial_memory_usage)
    end

    if Banyan.INVESTIGATING_MEMORY_USAGE
        @show task.scaled
        @show task.effects
        @show task.inputs
        @show task.outputs
        @show task.keep_same_sample_rate
    end

    # Get the final memory usage if it is not dependent on a constraint or other sample rates
    for fut in splatted_futures
        # Figure out if the future is mutated by this code region
        is_fut_mutated = task.effects[fut.value_id] == "MUT"

        # Get the final memory usage
        if !is_fut_mutated
            # For non-mutated data, we will only look at the initial
            # memory usage (in the scheduler) so it's fine to set the final
            # memory usage to the initial memory usage.
            task.memory_usage[fut.value_id]["final"] = task.memory_usage[fut.value_id]["initial"]
        else
            # Set memory usage based on a ScaleTo constraint if there is on
            final_memory_usage_set::Bool = false
            for c in task.memory_usage_constraints
                if startswith(c.type, "SCALE_TO=") && length(c.args) == 1 && c.args[1] == fut.value_id
                    final_memory_usage_set = true
                    task.memory_usage[fut.value_id]["final"] = parse(Int64, c.type[length("SCALE_TO=")+1:end])::Int64
                end
            end

            # If not and if this isn't scaled, then just set it to the sampled size if the
            # memory usage doesn't scale to larger values
            is_fut_scaled::Bool = false
            for f in task.scaled
                if fut.value_id == f.value_id
                    is_fut_scaled = true
                end
            end
            if Banyan.INVESTIGATING_MEMORY_USAGE
                @show is_fut_scaled final_memory_usage_set fut.value_id
            end
            if !final_memory_usage_set && !is_fut_scaled
                task.memory_usage[fut.value_id]["final"] = get_location(fut).sample.memory_usage
            end
        end
    end

    if Banyan.INVESTIGATING_MEMORY_USAGE
        @show task.memory_usage
    end

    # Apply SCALE_BY constraints to determine final memory usage
    for fut in splatted_futures
        if !haskey(task.memory_usage[fut.value_id], "final")
            for c in task.memory_usage_constraints
                if startswith(c.type, "SCALE_BY=") && length(c.args) == 2 && c.args[1] == fut.value_id
                    relative_to = c.args[2]
                    if haskey(task.memory_usage[relative_to], "final")
                        factor::Float64 = parse(Float64, c.type[10:end])
                        task.memory_usage[fut.value_id]["final"] = ceil(Int64, factor * task.memory_usage[relative_to]["final"])
                    end
                end
            end
        end
    end

    # Default case for determining memory usage
    if Banyan.INVESTIGATING_MEMORY_USAGE
        println("Computing memory usage for a new task")
    end
    for fut in splatted_futures
        if Banyan.INVESTIGATING_MEMORY_USAGE
            @show fut.value_id
        end
        if !haskey(task.memory_usage[fut.value_id], "final")
            total_sampled_input_memory_usage::Int64 = 0
            for scaled_fut in task.scaled
                if task.effects[scaled_fut.value_id] == "CONST"
                    total_sampled_input_memory_usage += get_location(scaled_fut).sample.memory_usage
                end
            end
            if Banyan.INVESTIGATING_MEMORY_USAGE
                @show total_sampled_input_memory_usage
            end
            if task.keep_same_sample_rate && total_sampled_input_memory_usage > 0
                # This case applies for most computation like `filter` and `groupby`

                total_input_memory_usage::Int64 = 0
                for fut in task.scaled
                    if task.effects[fut.value_id] == "CONST"
                        total_input_memory_usage += task.memory_usage[fut.value_id]["initial"]
                    end
                end
                if Banyan.INVESTIGATING_MEMORY_USAGE
                    @show total_input_memory_usage
                end

                # Use the sampels to figure out the rate of change in
                # memory usage going from inputs to outputs
                factor::Float64 = get_location(fut).sample.memory_usage / total_sampled_input_memory_usage
                if Banyan.INVESTIGATING_MEMORY_USAGE
                    @show factor
                end

                # Now we use that rate on the actual initial memory
                # usage which might have been modified using past memory
                # usage constraints like ScaleBy and ScaleTo.
                task.memory_usage[fut.value_id]["final"] = Base.convert(Int64, ceil(factor * total_input_memory_usage))::Int64
            elseif task.memory_usage[fut.value_id]["initial"] != 0
                # If the input is nonzero then the output is the same
                # because we don't support a change in memory usage that
                # isn't going from `nothing` to some assigned value.
                # This case applies to the very last code region created in
                # `partitioned_computation`.
                if Banyan.INVESTIGATING_MEMORY_USAGE
                    @show task.memory_usage[fut.value_id]["initial"]
                end
                task.memory_usage[fut.value_id]["final"] = task.memory_usage[fut.value_id]["initial"]
            else
                # This case applies for `fill` and `innerjoin`.
                fut_s::Sample =  get_location(fut).sample
                if Banyan.INVESTIGATING_MEMORY_USAGE
                    @show fut_s.memory_usage * fut_s.rate
                end
                task.memory_usage[fut.value_id]["final"] = fut_s.memory_usage * fut_s.rate
            end
        end
        if Banyan.INVESTIGATING_MEMORY_USAGE
            @show task.memory_usage[fut.value_id]
            @show fut.value_id
        end
    end

    # Compute additional memory usage
    for fut in splatted_futures
        additional_memory_usage::Int64 = 0
        for c in task.additional_memory_usage_constraints
            if startswith(c.type, "SCALE_TO=") && length(c.args) == 1 && c.args[1] == fut.value_id
                additional::Int64 = parse(Int64, c.type[length("SCALE_TO=")+1:end])
                additional_memory_usage += additional
            elseif startswith(c.type, "SCALE_BY=") && length(c.args) == 2 && c.args[1] == fut.value_id
                arg = c.args[2]
                factor::Float64 = parse(Float64, c.type[length("SCALE_BY=")+1:end])
                additional_memory_usage += ceil(Int64, factor * task.memory_usage[arg]["final"])
            end
        end
        task.memory_usage[fut.value_id]["additional"] = additional_memory_usage
    end

    # Ensure that all the outputs have the same sample rate
    output_sample_rate::Int64 = -1
    output_sample_rate_from_scaled = false
    is_anything_mutated = false
    for fut in splatted_futures
        is_fut_mutated = task.effects[fut.value_id] == "MUT"
        is_fut_scaled::Bool = false
        for f in task.scaled
            if fut.value_id == f.value_id
                is_fut_scaled = true
            end
        end
        is_anything_mutated = is_anything_mutated || is_fut_mutated
        if !output_sample_rate_from_scaled && is_fut_mutated
            output_sample_rate = get_location(fut).sample.rate
            output_sample_rate_from_scaled = is_fut_scaled
        end
    end
    (output_sample_rate != -1 || !is_anything_mutated) || error("Failed to compute output sample rate")
    for fut in splatted_futures
        # Skip over non-mutated futures and scaled futures
        if task.effects[fut.value_id] != "MUT"
            continue
        end
        for f in task.scaled
            if fut.value_id == f.value_id
                continue
            end
        end
        # Set sample rate for futures that are mutated and not scaled
        # (we already keep the same sample rate for scaled futures)
        get_location(fut).sample.rate = output_sample_rate
    end

    # Destroy value IDs that are no longer needed because of mutation
    for fut in splatted_futures
        fut.total_memory_usage = task.memory_usage[fut.value_id]["final"]

        # Issue destroy request for mutated futures that are no longer
        # going to be used
        is_fut_used::Bool = false
        fut_value_id = fut.value_id
        for f in keys(task.mutation)
            if fut_value_id == f.value_id
                is_fut_used = true
            end
        end
        is_fut_to_be_used::Bool = false
        for f in values(task.mutation)
            if fut_value_id == f.value_id
                is_fut_to_be_used = true
            end
        end
        if is_fut_used && !is_fut_to_be_used
            record_request(DestroyRequest(fut.value_id))
        end
    end

    # Record request to record task in backend's dependency graph and reset
    record_request(RecordTaskRequest(task))
    finish_task()

    # Make a call to `apply_mutation` to handle calls to `mut` like
    # `mutated(df, res)`
    apply_mutation(task.mutation, false)
end

function get_splatted_futures(unsplatted_futures::Vector{Union{Future,Vector{Future}}})::Vector{Future}
    splatted_futures::Vector{Future} = []
    for unsplatted_future in unsplatted_futures
        if unsplatted_future isa Vector
            unsplatted_future::Vector{Future}
            for uf in unsplatted_future
                push!(splatted_futures, uf)
            end
        else
            push!(splatted_futures, unsplatted_future)
        end
    end
    splatted_futures
end

function prepare_task_for_partitioned_code_region(
    unsplatted_futures::Vector{Union{Future,Vector{Future}}},
    unsplatted_variable_names::Vector{String},
    splatted_futures::Vector{Future},
    code::String
)
    splatted_variable_names = String[]
    task::DelayedTask = get_task()
    # Get code to initialize the unsplatted variable in the code region
    # TODO: Generate code in codegen that puts each code region in a
    # seperate function (where we reuse functions with the hash of the
    # function body) so that we don't have scoping-related bugs
    task.code = ""
    for j = 1:length(unsplatted_variable_names)
        unsplatted_variable_name::String = unsplatted_variable_names[j]
        task.code *= "$unsplatted_variable_name = "
        if unsplatted_futures[j] isa Vector
            uf::Vector{Future} = unsplatted_futures[j]
            task.code *= "["
            for i = 1:length(uf)
                splatted_variable_name = unsplatted_variable_name * "_$i"
                push!(splatted_variable_names, splatted_variable_name)
                task.code *= "$splatted_variable_name, "
            end
            task.code *= "]\n"
        else
            push!(splatted_variable_names, unsplatted_variable_name)
            task.code *= "$unsplatted_variable_name\n"
        end
    end
    task.code *= code
    if Banyan.INVESTIGATING_CODE_EXECUTION_FINISHING
        task.code *= "\nprintln(\"Finished code region on $(MPI.Initialized() ? MPI.Comm_rank(MPI.COMM_WORLD) : -1)\")\n"
    end
    task.value_names = map(
        x -> (x[1].value_id, x[2]),
        zip(splatted_futures, splatted_variable_names)
    )

    # Set `mutated` field of the `Future`s that have been mutated. This is
    # to ensure that future calls to `evaluate` on those `Future`s with
    # `mutated=true` and _only_ those `Future`s will result in an actual
    # evaluation
    for fut in splatted_futures
        is_task_mutated::Bool = false
        for m in values(task.mutation)
            if fut.value_id == m.value_id
                is_task_mutated = true
            end
        end
        task.effects[fut.value_id] = is_task_mutated ? "MUT" : "CONST"
    end
end

function reassign_futures(
    unsplatted_futures::Vector{Union{Future,Vector{Future}}},
    variables::Vector{Union{Any,Vector{Any}}}
)
    uf::Vector{Future} = Future[]
    for i in 1:length(unsplatted_futures)
        variable = variables[i]
        if unsplatted_futures[i] isa Vector
            uf = unsplatted_futures[i]
            for j = 1:length(uf)
                fe::Future = uf[j]
                setsample!(fe, variable[j])
            end
        else
            uf = Future[unsplatted_futures[i]]
            setsample!(uf[1], variable)
        end
    end
end

function partitioned_code_region(
    variables::Vector{Expr},
    variable_names::Vector{String},
    code::Expr,
    assigning_samples::Vector{Expr},
    # reassigning_futures::Vector{Expr}
)
    code_string::String = string(code)
    res = quote
        # Convert arguments to `Future`s if they aren't already
        unsplatted_futures::Vector{Union{Future,Vector{Future}}} =
            map(
                v -> if v isa Tuple
                    convert(Vector{Future}, Base.collect(v))
                else
                    convert(Future, v)
                end,
                [$(variables...)]
            )
        splatted_futures::Vector{Future} = get_splatted_futures(unsplatted_futures)    
        
        # TODO: Allow for any Julia object (even stuff that can't be converted
        # to `Future`s) to be passed into an @partitioned and by default have
        # it be replicated across all workers and batches
        # for pa in get_pa_union()
        #     for fut in futures
        #         if !(fut.value_id in keys(pa.partitions.pt_stacks))
        #             pa.partitions.pt_stacks[fut.value_id] = [Replicated()]
        #             # TODO: Add necessary default constraints so this
        #             # replication spans what everything else spans
        #         end
        #     end
        # end

        # If any sample computation fails, before we rethrow
        # the error (so that it is displayed in a notebook or crashes a
        # script/Lambda function that is running the session) we ensure that
        # we haven't recorded a faulty task or messed up the state in any way.

        # Fill in task with code and value names pulled using the macror
        unsplatted_variable_names::Vector{String} = String[$(variable_names...)]
        
        code::String = $code_string
        prepare_task_for_partitioned_code_region(
            unsplatted_futures,
            unsplatted_variable_names,
            splatted_futures,
            code
        )

        # TODO: When mutating a value, ensure that the old future has a sample
        # of the old value and the new future of the new

        # Perform computation on samples
        try
            let ($(variables...),) = [$(assigning_samples...)]
                begin
                    # Run the computation
                    $(esc(code))
                    # Move results from variables back into the samples. Also, update the
                    # memory usage accordingly.
                    # TODO: Determine if other sample properties need to be invalidated (or
                    # updated) after modified by an annotated code region.
                    # $(reassigning_futures...)
                    reassign_futures(unsplatted_futures, Any[$(variables...)])
                end
            end
        catch
            # I
            finish_task()
            rethrow()
        end

        # NOTE: We only update futures' state, record requests, update samples,
        # apply mutation _IF_ the sample computation succeeds. Regardless of
        # whether it succeeds, we make sure to clear the task (so that future
        # usage in the REPL or notebook can proceed, generating new tasks) and
        # reassign all variables.
        finish_partitioned_code_region(splatted_futures)
    end
    res
end

get_samples(ufs::Base.Vector{Future}) = map(sample, ufs)
get_samples(uf::Future) = sample(uf)

macro partitioned(ex...)
    # Load in variables and code from macro
    vars::Vector{Symbol} = Base.collect(ex[1:end-1])
    variables::Vector{Expr} = map(esc, vars)
    variable_names::Vector{String} = map(string, vars)
    code::Expr = ex[end]

    # What are splatted futures? This is basically the concept that you might
    # have a variable-length list of futures that you need to pass into an
    # annotated code region. We handle this by alowing you to pass in a `Vector{Future}`
    # in the "arguments" of @partitioned. We will then splat the futures into an array
    # with variable names generated according to the index of the future in
    # the list of futures they came from.

    assigning_samples = Expr[]
    # reassigning_futures = Expr[quote uf::Vector{Future} = Future[] end]
    for i in 1:length(variables)
        # Assign samples to variables used in annotated code
        push!(
            assigning_samples,
            quote get_samples(unsplatted_futures[$i]) end
        )
    end

    res = partitioned_code_region(
        variables,
        variable_names,
        code,
        assigning_samples,
    )
    res
end

############################################################################
# Helper functions for compiling PAs to send as part of tasks in `compute` #
############################################################################

function duplicate_arg(arg::PartitionTypeReference, pa::PartitionAnnotation)::PartitionTypeReference
    v::ValueId, idx::Int64 = arg
    (v, idx + div(length(pa.partitions.pt_stacks[v].pts), 2))
end

function duplicate_args(
    args::Vector{PartitionTypeReference},
    pa::PartitionAnnotation,
)::Vector{PartitionTypeReference}
    map(arg -> duplicate_arg(arg, pa), args)
end

function apply_default_constraints!(pa::PartitionAnnotation)
    # Add Cross constraints for all unconstrained PTs
    for (v, pt_stack) in pa.partitions.pt_stacks
        for i::Int64 = 1:length(pt_stack.pts)
            # Check if in_cross_or_co
            in_cross_or_co::Bool = false
            for c in pa.constraints.constraints
                if (c.type == "CROSS" || c.type == "CO") && (v, i - 1) in c.args
                    in_cross_or_co = true
                elseif c.type == "CO_GROUP" && any((v, i - 1) in group for group in c.args)
                    in_cross_or_co = true
                end
            end

            # Add Cross constraint for those not constrained in any way
            if !in_cross_or_co
                push!(
                    pa.constraints.constraints,
                    PartitioningConstraintOverGroup(
                        "CROSS", PartitionTypeReference[(v, i - 1)]
                    ),
                )
            end
        end
    end

    # Add Co constraint for all Cross-ed PTs

    # Find all co-partitioned PTs
    # inv: Every PT has been Cross-ed
    co_args = PartitionTypeReference[]
    co_group_args = Vector{PartitionTypeReference}[]
    for c in pa.constraints.constraints
        if c.type == "CROSS"
            if length(c.args) == 1
                push!(co_args, c.args[1])
            elseif length(c.args) > 1
                push!(co_group_args, deepcopy(c.args))
            end
        end
    end
    if length(co_group_args) == 1 && length(co_args) > 0
        push!(co_group_args, co_args[1:1])
    end

    # Add constraints
    if length(co_args) > 0
        push!(pa.constraints.constraints, PartitioningConstraintOverGroup("CO", co_args))
    end
    if length(co_group_args) > 0
        push!(
            pa.constraints.constraints,
            PartitioningConstraintOverGroups("CO_GROUP", co_group_args),
        )
    end

    # TODO: Add MemoryUsage constraint by default which computes sample size or
    # defaults to 0
end

function duplicated_constraints_for_batching(pc::PartitioningConstraints, pa::PartitionAnnotation)::PartitioningConstraints
    new_pcs = PartitioningConstraint[]
    for c in pc.constraints
        c_type::String = c.type
        if c_type == "CO" ||
            c_type == "EQUAL" ||
            c_type == "SEQUENTIAL" ||
            c_type == "MATCH" ||
            startswith(c.type, "MATCH_ON=")
            push!(new_pcs, deepcopy(c))
            push!(new_pcs, PartitioningConstraintOverGroup(c_type, duplicate_args(c.args, pa)))
        elseif c_type == "CROSS" || startswith(c_type, "AT_MOST=")
            # Note that with Cross constraints, the order of the
            # arguments matters. But actually that doesnt matter.
            # The scheduler will automaticcally ensure that the order
            # of PTs in a PT stack is obeyed.
            # ||
            # c.type == "MATCH" || startswith(c.type, "MATCH_ON")
            new_c_args::Vector{PartitionTypeReference} = deepcopy(c.args)
            append!(new_c_args, duplicate_args(c.args, pa))
            push!(
                new_pcs,
                PartitioningConstraintOverGroup(
                    c_type,
                    new_c_args,
                )
            )
        elseif c_type == "CO_GROUP"
            new_co_args::Vector{Vector{PartitionTypeReference}} = Vector{PartitionTypeReference}[]
            for group::Vector{PartitionTypeReference} in c.co_args
                push!(new_co_args, duplicate_args(group, pa))
            end
            push!(
                new_pcs,
                PartitioningConstraintOverGroups(
                    c_type,
                    new_co_args
                ),
            )
        elseif startswith(c_type, "SCALE_BY=")
            # `ScaleBy` constraints are not duplicated. They must refer to
            # only the first PT of the PT compositions they reference.
            push!(new_pcs, c)
        end
    end
    PartitioningConstraints(new_pcs)
end

function duplicate_for_batching!(pa::PartitionAnnotation)
    # Duplicate PT stacks
    for pt_stack::PartitionTypeComposition in values(pa.partitions.pt_stacks)
        # Copy over the PT stack
        second_half::Vector{PartitionType} = deepcopy(pt_stack.pts)

        # Append to form a compositions of PTs that is twic in length
        append!(pt_stack.pts, second_half)
    end

    # Duplicate annotation-level constraints for Co, Equal, Cross, AtMost, ScaleBy
    pa.constraints = duplicated_constraints_for_batching(pa.constraints, pa)

    # Add constraints for second half being Sequential and Match-ing the first
    # half
    for (v, pt_stack::PartitionTypeComposition) in pa.partitions.pt_stacks
        for i = 1:div(length(pt_stack.pts), 2)
            dupi = i + div(length(pt_stack.pts), 2)

            # Add in `Sequential` and `Match` constraints for the duplicated
            # part of the PT composition
            push!(
                pa.constraints.constraints,
                PartitioningConstraintOverGroup("SEQUENTIAL", [(v, dupi - 1)]),
            )
            # Since we have duplicated all constraints, we don't need to
            # constrain both halves of the PT composition to match

            # Duplicate PT-level constraints
            pt_dup::PartitionType = pt_stack.pts[dupi]
            pt_stack.pts[dupi].constraints =
                duplicated_constraints_for_batching(pt_dup.constraints, pa)
        end
    end
end
