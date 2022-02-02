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

function get_task()
    global curr_delayed_task
    curr_delayed_task
end

function finish_task()
    global curr_delayed_task
    curr_delayed_task = DelayedTask()
end

function get_pa()
    global curr_delayed_task
    last(curr_delayed_task.pa_union)
end

function finish_pa()
    global curr_delayed_task
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

function partitioned_using(handler::Function)
    global curr_delayed_task
    curr_delayed_task.partitioned_using_func = handler
end

function keep_all_sample_keys(participants::AbstractFuture...; drifted = false)
    # This can be use on an input and output that share the same keys that they
    # can be grouped by.

    # Take the union of discovered grouping keys. Grouping keys are discovered
    # through calls to `keep_keys` in functions where we know that we need
    # to allow grouping by certain keys such as sorting and join functions.
    # println("In keep_all_sample_keys")
    # @show participants
    # @show [sample(p, :groupingkeys) for p in participants]
    groupingkeys = union([sample(p, :groupingkeys) for p in participants]...)
    # @show groupingkeys
    statistics = if !drifted
        merge([sample(p, :statistics) for p in participants]...)
    else
        nothing
    end

    # Only allow keys that are actually in the keys of the participants
    # TODO: Maybe replace this with a function that doesn't require calling
    # a potentially expensive function to iterate through _all_ columns.
    for p in participants
        p_keys = intersect(groupingkeys, sample(p, :keys))
        setsample!(p, :groupingkeys, p_keys)
        if !drifted
            setsample!(
                p,
                :statistics,
                Dict(
                    key => statistic for
                    (key, statistic) in statistics if key in sample(p, :keys)
                ),
            )
        end
    end
end

function keep_all_sample_keys_renamed(old::AbstractFuture, new::AbstractFuture)
    for (old_key, new_key) in zip(sample(old, :keys), sample(new, :keys))
        if old_key in sample(old, :groupingkeys)
            setsample!(new, :groupingkeys, union(sample(new, :groupingkeys), [new_key]))
            setsample!(new, :statistics, new_key, sample(old, :statistics, old_key))
        end
        if new_key in sample(new, :groupingkeys)
            setsample!(old, :groupingkeys, union(sample(old, :groupingkeys), [old_key]))
            setsample!(old, :statistics, old_key, sample(new, :statistics, new_key))
        end
    end
end

function keep_sample_keys_named(
    participants::Pair{<:AbstractFuture,<:Any}...;
    drifted = false,
)
    # println("In keep_sample_keys_named")
    # `participants` maps from futures to lists of key names such that all
    # participating futures have the same sample properties for the keys at
    # same indices in those lists
    participants = [
        (participant, Symbol.(to_vector(key_names))) for
        (participant, key_names) in participants
    ]
    # @show participants
    nkeys = length(first(participants)[2])
    for i = 1:nkeys
        # Copy over allowed grouping keys
        for (p, keys) in participants
            p_key = keys[i]
            # @show p
            # @show p_key
            # @show union(sample(p, :groupingkeys), [p_key])
            setsample!(p, :groupingkeys, union(sample(p, :groupingkeys), [p_key]))
        end

        # Copy over statistics if they haven't changed
        if !drifted
            key_statistics =
                merge([sample(p, :statistics, keys[i]) for (p, keys) in participants]...)
            for (p, keys) in participants
                setsample!(p, :statistics, key_statistics)
            end
        end
    end
end

# NOTE: For operations where join rate and data skew might be different, it is
# assumed that they are the same. For example, column vectors from the result
# of a join should have the same sample rate and the same data skew.

keep_sample_keys(keys, participants::AbstractFuture...; drifted = false) = begin
    keys = Symbol.(to_vector(keys))
    keep_sample_keys_named([p => keys for p in participants]..., drifted = drifted)
end

# This is useful for workloads that involve joins where the sample rate is
# diminished quadratically for each joinv
# TODO: Make _this_ more accurate for `innerjoin``
keep_sample_rate(fut::AbstractFuture, relative_to::AbstractFuture...) =
    setsample!(fut, :rate, prod([sample(r, :rate) for r in relative_to]))

###############################
# Using samples to assign PTs #
###############################

function partitioned_with(
    handler::Function;
    # Memory usage, sampling
    # `scaled` is the set of futures with memory usage that can potentially be
    # scaled to larger sizes if the amount of data at a location changes.
    # Non-scaled data has fixed memory usage regardless of its sample rate.
    scaled::Union{AbstractFuture,Vector{<:AbstractFuture}} = AbstractFuture[],
    keep_same_sample_rate::Bool = true,
    memory_usage::Vector{PartitioningConstraint} = PartitioningConstraint[],
    additional_memory_usage::Vector{PartitioningConstraint} = PartitioningConstraint[],
    # Keys (not relevant if you never use grouped partitioning).
    grouped::Union{Nothing,AbstractFuture,Vector{<:AbstractFuture}} = nothing,
    keep_same_keys::Bool = false,
    keys::Union{AbstractVector,Nothing} = nothing,
    keys_by_future = nothing,
    renamed::Bool = false,
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted::Bool = false,
    # For generating import statements
    modules::Union{String,AbstractVector{String},Nothing} = nothing
)
    curr_delayed_task = get_task()

    if !isnothing(modules)
        modules = to_vector(modules)
        partitioned_using_modules(modules...)
    end

    scaled = [convert(Future, f) for f in to_vector(scaled)]
    curr_delayed_task.scaled = scaled
    curr_delayed_task.partitioned_with_func = handler
    curr_delayed_task.keep_same_sample_rate = keep_same_sample_rate
    curr_delayed_task.memory_usage_constraints = memory_usage
    curr_delayed_task.additional_memory_usage_constraints = additional_memory_usage

    # TODO: Ensure partitioned_using is able to capture updates to the task when mutating

    partitioned_using() do
        # Categorize the futures. We determine whether a future is an input by
        # checking if it's not in the values of the task's mutation. All
        # outputs would require mutation for the future to be an output. And
        # that would have been marked by a call to `mutated` that is made in the
        # `Future` constructor.
        curr_delayed_task = get_task()
        outputs = [f for f in curr_delayed_task.scaled if any((f.value_id == ff.value_id for ff in values(curr_delayed_task.mutation)))]
        inputs = [f for f in curr_delayed_task.scaled if !any((f.value_id == ff.value_id for ff in outputs))]
        grouping_needed = keep_same_keys || !isnothing(keys) || !isnothing(keys_by_future) || renamed
        if grouping_needed
            grouped = isnothing(grouped) ? vcat(inputs, outputs) : [convert(Future, f) for f in to_vector(grouped)]
            outputs_grouped = [f for f in grouped if any((f.value_id == ff.value_id for ff in outputs))]
            inputs_grouped = [f for f in grouped if any((f.value_id == ff.value_id for ff in inputs))]
        end

        # Propagate information about keys that can be used for grouping
        if keep_same_keys
            if renamed
                if length(inputs_grouped) != 1 || length(outputs_grouped) != 1
                    error("Only 1 argument can be renamed to 1 result at once")
                end
                keep_all_sample_keys_renamed(inputs_grouped[1], outputs_grouped[1])
            else
                # @show inputs
                # @show outputs
                # @show grouped
                # @show inputs_grouped
                # @show outputs_grouped
                keep_all_sample_keys(vcat(outputs_grouped, inputs_grouped)...; drifted=drifted)
            end
        end
        if !isnothing(keys)
            keep_sample_keys(keys, vcat(outputs_grouped, inputs_grouped)...; drifted=drifted)
        end
        if !isnothing(keys_by_future)
            keep_sample_keys_named(keys_by_future...; drifted=drifted)
        end

        # Propgate sample rates
        if !isempty(inputs)
            if keep_same_sample_rate
                for r in outputs
                    keep_sample_rate(r, first(inputs))
                end
                for i in 1:(length(inputs)-1)
                    this_sample_rate = sample(inputs[i], :rate)
                    other_sample_rate = sample(inputs[i+1], :rate)
                    if this_sample_rate != other_sample_rate
                        @warn "Two inputs have different sample rates ($this_sample_rate, $other_sample_rate)"
                    end
                end
            else
                for r in outputs
                    keep_sample_rate(r, inputs...)
                end
            end
        end

        # Store the important inputs and outputs for scaling memory usage
        curr_delayed_task.inputs = inputs
        curr_delayed_task.outputs = outputs
    end
end

function pt(
    args::Union{AbstractFuture,PartitionType,PartitionTypeComposition,Vector}...;
    kwargs...,
)
    pa = get_pa()

    # Extract PT and args to assign the PT to from given arguments
    futs, sharedptype = args[1:end-1], last(args)

    if length(futs) > 1 && sharedptype isa Vector && length(sharedptype) > 1
        throw(
            ArgumentError(
                "Multiple partition types cannot be applied to multiple futures at once",
            ),
        )
    end

    # Assign PT to each future
    for fut in futs
        # Start new PA if this assignment would overwrite one for the current
        # PA. When we start a new PA, we append the old one the PA union for
        # the task being currently constructed.
        fut = convert(Future, fut)
        if fut.value_id in keys(pa.partitions.pt_stacks)
            finish_pa()
            pa = get_pa()
        end

        # Take a copy so that we can mutate
        ptype = deepcopy(sharedptype)

        # Put in PT
        if ptype isa Vector
            for pty in ptype
                pt(fut, PartitionTypeComposition([pty]); kwargs...)
            end
        elseif ptype isa PartitionType
            pt(fut, PartitionTypeComposition([ptype]); kwargs...)
        elseif ptype isa Vector{PartitionType} && length(ptype) == 1
            pt(fut, PartitionTypeComposition(ptype); kwargs...)
        elseif ptype isa PartitionTypeComposition
            # Handle constraints that have been delayed till PT assignment
            for pty in ptype.pts
                for (i, constraint) in enumerate(pty.constraints.constraints)
                    if constraint isa Function
                        pty.constraints.constraints[i] = constraint(fut)
                    end
                end
            end

            # Add to PAs information about how partitions are produced
            pa.partitions.pt_stacks[fut.value_id] = ptype

            # Add to PAs information about how partitioning is constrained
            for c in get(kwargs, :constrain, [])
                push!(pa.constraints.constraints, c)
            end

            # Handle `match`, `on` in keyword arguments
            if :match in keys(kwargs) && !isnothing(kwargs[:match])
                to_match_with = to_vector(kwargs[:match])
                if :on in keys(kwargs) && !isnothing(kwargs[:on])
                    for to_match_on in to_vector(get(kwargs, :on, []))
                        push!(
                            pa.constraints.constraints,
                            MatchOn(to_match_on, fut, to_match_with...),
                        )
                    end
                else
                    push!(pa.constraints.constraints, Match(fut, to_match_with...))
                end
            end

            if :cross in keys(kwargs) && !isnothing(kwargs[:cross])
                to_cross = to_vector(kwargs[:match])
                push!(pa.constraints.constraints, Cross(to_cross...))
            end

            # TODO: Implement support for other constraints in kwargs
        else
            throw(
                ArgumentError(
                    "Expected partition type (PT) or a composition or union of PTs",
                ),
            )
        end
    end
end

# NOTE: `mutated` should not be used inside of `partitioned_with` or
# `partitioned_using`

mutated(f::AbstractFuture) = mutated(f => f)
mutated(ff::Pair{<:AbstractFuture,<:AbstractFuture}) = mutated(first(ff), last(ff))

function mutated(old::AbstractFuture, new::AbstractFuture)
    global curr_delayed_task
    old = convert(Future, old)
    # if haskey(curr_delayed_task.value_names, old.value_id)
    #     finish_pa()
    curr_delayed_task.mutation[old] = convert(Future, new)
end

#################################################
# Macro for wrapping the code region to offload #
#################################################

function apply_mutation(mutation::Dict{Future,Future})
    for (old, new) in mutation
        if old != new
            # Apply the mutation by setting the value ID of the old future the
            # value ID of the new one. That way, the user can continue using
            # the old future as if it were mutated but it will be having a
            # different value ID.

            # Swap (1) references in `futures_on_client` if either side of the
            # mutation is on the client
            futures_on_client = get_session().futures_on_client
            if old.value_id in keys(futures_on_client) &&
               new.value_id in keys(futures_on_client)
                futures_on_client[new.value_id], futures_on_client[old.value_id] =
                    futures_on_client[old.value_id], futures_on_client[new.value_id]
            elseif old.value_id in keys(futures_on_client)
                futures_on_client[new.value_id] = futures_on_client[old.value_id]
                delete!(futures_on_client, old.value_id)
            elseif new.value_id in keys(futures_on_client)
                futures_on_client[old.value_id] = futures_on_client[new.value_id]
                delete!(futures_on_client, new.value_id)
            end

            # Swap (2) other fields of the `Future`s and (3) their locations
            session_locations = get_session().locations
            (
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
                session_locations[new.value_id],
            ) = (
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
                session_locations[old.value_id],
            )
        end
    end
end

invert(mutation::Dict{Future,Future}) = Dict(new => old for (old, new) in mutation)

function partitioned_using_modules(m...)
    global curr_delayed_task
    union!(curr_delayed_task.used_modules, m)
end

macro partitioned(ex...)
    res = quote end

    # Load in variables and code from macro
    variables = [esc(e) for e in ex[1:end-1]]
    variable_names = [string(e) for e in ex[1:end-1]]
    code = ex[end]

    # What are splatted futures? This is basically the concept that you might
    # have a variable-length list of futures that you need to pass into an
    # annotated code region. We handle this by alowing you to pass in a `Vector{Future}`
    # in the "arguments" of @partitioned. We will then splat the futures into an array
    # with variable names generated according to the index of the future in
    # the list of futures they came from.

    # Assign samples to variables used in annotated code
    assigning_samples = [
        quote
            unsplatted_future = unsplatted_futures[$i]
            $variable =
                unsplatted_future isa Tuple ? sample.(unsplatted_future) :
                sample(unsplatted_future)
        end for (i, variable) in enumerate(variables)
    ]

    # Re-assign futures to variables that were used in annotated code
    # TODO: Ensure that it is okay for different `quote...end` blocks to refer
    # to the same variable name. They shouldn't have different gensym-ed
    # variable names in the macro expansion.
    reassigning_futures = [
        quote
            $variable = unsplatted_futures[$i]
        end for (i, variable) in enumerate(variables)
    ]

    quote
        # $res

        # Convert arguments to `Future`s if they aren't already
        unsplatted_futures = [$(variables...)]
        splatted_futures::Vector{Future} = []
        for unsplatted_future in unsplatted_futures
            if unsplatted_future isa Tuple
                for uf in unsplatted_future
                    push!(splatted_futures, convert(Future, uf))
                end
            else
                push!(splatted_futures, convert(Future, unsplatted_future))
            end
        end

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
        unsplatted_variable_names = [$(variable_names...)]
        splatted_variable_names = []
        task = get_task()
        # Get code to initialize the unsplatted variable in the code region
        # TODO: Generate code in codegen that puts each code region in a
        # seperate function (where we reuse functions with the hash of the
        # function body) so that we don't have scoping-related bugs
        task.code = ""
        for (variable, unsplatted_variable_name) in
            zip(unsplatted_futures, unsplatted_variable_names)
            task.code *= "$unsplatted_variable_name = "
            if variable isa Tuple
                task.code *= "["
                for (i, v) in enumerate(variable)
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
        task.code *= $(string(code))
        if isinvestigating()[:code_execution][:finishing]
            task.code *= "\nprintln(\"Finished code region on $(MPI.Initialized() ? MPI.Comm_rank(MPI.COMM_WORLD) : -1)\")\n"
        end
        task.value_names = [
            (fut.value_id, var_name) for (fut, var_name) in
            zip(splatted_futures, splatted_variable_names)
        ]

        # Set `mutated` field of the `Future`s that have been mutated. This is
        # to ensure that future calls to `evaluate` on those `Future`s with
        # `mutated=true` and _only_ those `Future`s will result in an actual
        # evaluation
        for fut in splatted_futures
            if any((fut.value_id == m.value_id for m in values(task.mutation)))
                task.effects[fut.value_id] = "MUT"
            else
                task.effects[fut.value_id] = "CONST"
            end
        end

        # TODO: When mutating a value, ensure that the old future has a sample
        # of the old value and the new future of the new

        # Perform computation on samples
        $(assigning_samples...)      
        try
            $(esc(code))
        catch
            # I
            finish_task()
            $(reassigning_futures...)
            rethrow()
        end

        # NOTE: We only update futures' state, record requests, update samples,
        # apply mutation _IF_ the sample computation succeeds. Regardless of
        # whether it succeeds, we make sure to clear the task (so that future
        # usage in the REPL or notebook can proceed, generating new tasks) and
        # reassign all variables.

        # Update mutated futures
        for fut in splatted_futures
            if any((fut.value_id == m.value_id for m in values(task.mutation)))
                fut.stale = true
                fut.mutated = true
            end
        end

        # Apply all delayed source and destination assignment. This will
        # perform any expensive sample collection that may require for example
        # an expensive scan of S3. This will record `RecordLocationRequest`s.
        for splatted_future in splatted_futures
            apply_sourced_or_destined_funcs(splatted_future)
        end

        # Move results from variables back into the samples. Also, update the
        # memory usage accordingly.
        # TODO: Determine if other sample properties need to be invalidated (or
        # updated) after modified by an annotated code region.
        for (f, value) in zip(unsplatted_futures, [$(variables...)])
            if f isa Tuple
                for (fe, ve) in zip(f, value)
                    setsample!(fe, ve)
                    setsample!(fe, :memory_usage, sample_memory_usage(ve))
                end
            else
                setsample!(f, value)
                setsample!(f, :memory_usage, sample_memory_usage(value))
            end
        end

        # Look at mutation, inputs, outputs, and constraints to determine
        # initial/final/additional memory usage and also to issue destroy
        # requests for all mutated values. Maybe also set to nothing and
        # assign new value here for mutation. Also set new future's total
        # memory usage.

        # Get the initial memory usage
        for fut in splatted_futures
            fut_initial_memory_usage = if !isnothing(fut.total_memory_usage)
                fut.total_memory_usage
            else
                get_location(fut).total_memory_usage
            end
            !isnothing(fut_initial_memory_usage) || error("Future with value ID $(fut.value_id) has no initial memory usage even in location with source name $(get_location(fut).src_name)")
            task.memory_usage[fut.value_id] = Dict("initial" => fut_initial_memory_usage)
        end

        # Get the final memory usage if it is not dependent on a constraint or other sample rates
        for fut in splatted_futures
            # Figure out if the future is mutated by this code region
            is_fut_mutated = task.effects[fut.value_id] == "MUT"

            # Get the final memory usage
            fut_final_memory_usage = if !is_fut_mutated
                # For non-mutated data, we will only look at the initial
                # memory usage (in the scheduler) so it's fine to set the final
                # memory usage to the initial memory usage.
                task.memory_usage[fut.value_id]["final"] = task.memory_usage[fut.value_id]["initial"]
            elseif any(startswith(c.type, "SCALE_TO=") && fut.value_id in c.args for c in task.memory_usage_constraints)
                for c in task.memory_usage_constraints
                    if startswith(c.type, "SCALE_TO=") && length(c.args) == 1 && c.args[1] == fut.value_id
                        task.memory_usage[fut.value_id]["final"] = Base.convert(Integer, ceil(parse(Int32, c.type[length("SCALE_TO=")+1:end])))
                    end
                end
            elseif !any(fut.value_id == f.value_id for f in task.scaled)
                task.memory_usage[fut.value_id]["final"] = sample(fut, :memory_usage)
            end
        end

        # Apply SCALE_BY constraints to determine final memory usage
        for fut in splatted_futures
            if !haskey(task.memory_usage[fut.value_id], "final")
                for c in task.memory_usage_constraints
                    if startswith(c.type, "SCALE_BY=") && length(c.args) == 2 && c.args[1] == fut.value_id
                        factor = parse(Int32, c.type[length("SCALE_BY=")+1:end])
                        relative_to = c.args[2]
                        if haskey(task.memory_usage[relative_to], "final")
                            task.memory_usage[fut.value_id]["final"] = Base.convert(Integer, ceil(factor * task.memory_usage[relative_to]["final"]))
                        end
                    end
                end
            end
        end

        # Default case for determining memory usage
        if isinvestigating()[:memory_usage]
            println("Computing memory usage for a new task")
        end
        for fut in splatted_futures
            if isinvestigating()[:memory_usage]
                @show fut.value_id
            end
            if !haskey(task.memory_usage[fut.value_id], "final")
                total_sampled_input_memory_usage = sum((
                    sample(fut, :memory_usage)
                    for fut in task.scaled
                    if task.effects[fut.value_id] == "CONST"
                ), init=0)
                if isinvestigating()[:memory_usage]
                    @show total_sampled_input_memory_usage
                end
                if task.keep_same_sample_rate && total_sampled_input_memory_usage > 0
                    # This case applies for most computation like `filter` and `groupby`

                    total_input_memory_usage = sum((
                        task.memory_usage[fut.value_id]["initial"]
                        for fut in task.scaled
                        if task.effects[fut.value_id] == "CONST"
                    ), init=0)
                    if isinvestigating()[:memory_usage]
                        @show total_input_memory_usage
                    end

                    # Use the sampels to figure out the rate of change in
                    # memory usage going from inputs to outputs
                    factor = sample(fut, :memory_usage) / total_sampled_input_memory_usage
                    if isinvestigating()[:memory_usage]
                        @show factor
                    end

                    # Now we use that rate on the actual initial memory
                    # usage which might have been modified using past memory
                    # usage constraints like ScaleBy and ScaleTo.
                    task.memory_usage[fut.value_id]["final"] = Base.convert(Integer, ceil(factor * total_input_memory_usage))
                    # @show total_sampled_input_memory_usage factor total_input_memory_usage task.memory_usage[fut.value_id]["final"]
                elseif task.memory_usage[fut.value_id]["initial"] != 0
                    # If the input is nonzero then the output is the same
                    # because we don't support a change in memory usage that
                    # isn't going from `nothing` to some assigned value.
                    # This case applies to the very last code region created in
                    # `partitioned_computation`.
                    if isinvestigating()[:memory_usage]
                        @show task.memory_usage[fut.value_id]["initial"]
                    end
                    task.memory_usage[fut.value_id]["final"] = task.memory_usage[fut.value_id]["initial"]
                else
                    # This case applies for `fill` and `innerjoin`.
                    if isinvestigating()[:memory_usage]
                        @show Base.convert(Integer, ceil(sample(fut, :memory_usage) * sample(fut, :rate)))
                    end
                    task.memory_usage[fut.value_id]["final"] = Base.convert(Integer, ceil(sample(fut, :memory_usage) * sample(fut, :rate)))
                end
            end
            if isinvestigating()[:memory_usage]
                @show task.memory_usage[fut.value_id]
                @show fut.value_id
            end
        end

        # Compute additional memory usage
        for fut in splatted_futures
            additional_memory_usage = 0
            for c in task.additional_memory_usage_constraints
                if startswith(c.type, "SCALE_TO=") && length(c.args) == 1 && c.args[1] == fut.value_id
                    additional = parse(Int32, c.type[length("SCALE_TO=")+1:end])
                    additional_memory_usage += Base.convert(Integer, ceil(additional))
                elseif startswith(c.type, "SCALE_BY=") && length(c.args) == 2 && c.args[1] == fut.value_id
                    arg = c.args[2]
                    factor = parse(Int32, c.type[length("SCALE_BY=")+1:end])
                    additional_memory_usage += factor * task.memory_usage[arg]["final"]
                end
            end
            task.memory_usage[fut.value_id]["additional"] = Base.convert(Integer, ceil(additional_memory_usage))
        end

        # Ensure that all the outputs have the same sample rate
        output_sample_rate = nothing
        output_sample_rate_from_scaled = false
        is_anything_mutated = false
        for fut in splatted_futures
            is_fut_mutated = task.effects[fut.value_id] == "MUT"
            is_fut_scaled = any(fut.value_id == f.value_id for f in task.scaled)
            is_anything_mutated = is_anything_mutated || is_fut_mutated
            if !output_sample_rate_from_scaled && is_fut_mutated
                output_sample_rate = sample(fut, :rate)
                output_sample_rate_from_scaled = is_fut_scaled
            end
        end
        !isnothing(output_sample_rate) || !is_anything_mutated || error("Failed to compute output sample rate")
        for fut in splatted_futures
            is_fut_mutated = task.effects[fut.value_id] == "MUT"
            is_fut_scaled = any(fut.value_id == f.value_id for f in task.scaled)
            if is_fut_mutated && !is_fut_scaled
                setsample!(fut, :rate, output_sample_rate)
            end
        end

        # Destroy value IDs that are no longer needed because of mutation
        for fut in splatted_futures
            fut.total_memory_usage = task.memory_usage[fut.value_id]["final"]

            # Issue destroy request for mutated futures that are no longer
            # going to be used
            if any((fut.value_id == f.value_id for f in keys(task.mutation))) && !any((fut.value_id == f.value_id for f in values(task.mutation)))
                record_request(DestroyRequest(fut.value_id))
            end
        end

        # Record request to record task in backend's dependency graph and reset
        record_request(RecordTaskRequest(task))
        finish_task()

        # This basically undoes `$(assigning_samples...)`.
        $(reassigning_futures...)

        # Make a call to `apply_mutation` to handle calls to `mut` like
        # `mutated(df, res)`
        apply_mutation(task.mutation)
    end
end

############################################################################
# Helper functions for compiling PAs to send as part of tasks in `compute` #
############################################################################

function duplicate_args(
    args::Vector{PartitionTypeReference},
    pa::PartitionAnnotation,
)::Vector{PartitionTypeReference}
    [(v, idx + div(length(pa.partitions.pt_stacks[v].pts), 2)) for
    # (v, idx + length(pa.partitions.pt_stacks[v].pts)) for
     (v, idx) in args]
end

function apply_default_constraints!(pa::PartitionAnnotation)
    # Add Cross constraints for all unconstrained PTs
    for (v, pt_stack) in pa.partitions.pt_stacks
        for i = 1:length(pt_stack.pts)
            # Check if in_cross_or_co
            in_cross_or_co = false
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
                    PartitioningConstraintOverGroup("CROSS", [(v, i - 1)]),
                )
            end
        end
    end

    # Add Co constraint for all Cross-ed PTs

    # Find all co-partitioned PTs
    # inv: Every PT has been Cross-ed
    co_args = []
    co_group_args = []
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
        push!(co_group_args, [co_args[1]])
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

duplicated_constraints_for_batching(pc::PartitioningConstraints, pa::PartitionAnnotation) =
    begin
        # promote(
        #     Union{PartitioningConstraint, Function}[],
        new_pts = vcat(
            [
                if c.type == "CO" ||
                   c.type == "EQUAL" ||
                   c.type == "SEQUENTIAL" ||
                   c.type == "MATCH" ||
                   startswith(c.type, "MATCH_ON=")
                    [
                        deepcopy(c),
                        PartitioningConstraintOverGroup(c.type, duplicate_args(c.args, pa)),
                    ]
                elseif c.type == "CROSS" || startswith(c.type, "AT_MOST=")
                    # Note that with Cross constraints, the order of the
                    # arguments matters. But actually that doesnt matter.
                    # The scheduler will automaticcally ensure that the order
                    # of PTs in a PT stack is obeyed.
                    # ||
                    # c.type == "MATCH" || startswith(c.type, "MATCH_ON")
                    [
                        PartitioningConstraintOverGroup(
                            c.type,
                            [deepcopy(c.args); duplicate_args(c.args, pa)],
                        ),
                    ]
                elseif c.type == "CO_GROUP"
                    [
                        PartitioningConstraintOverGroups(
                            c.type,
                            [duplicate_args(group, pa) for group in c.args],
                        ),
                    ]
                elseif startswith(c.type, "SCALE_BY=")
                    # `ScaleBy` constraints are not duplicated. They must refer to
                    # only the first PT of the PT compositions they reference.
                    [c]
                else
                    []
                end for c in pc.constraints
            ]...,
        )
        # println()
        # println(new_pts)
        # )
        PartitioningConstraints(new_pts)
    end

function duplicate_for_batching!(pa::PartitionAnnotation)
    # Duplicate PT stacks
    for (v, pt_stack) in pa.partitions.pt_stacks
        # Copy over the PT stack
        second_half = deepcopy(pt_stack.pts)

        # # Don't duplicate parameters that have bang values
        # for pt in second_half
        #     for (k, v) in pt.parameters
        #         if v == "!"
        #             pop!(pt.parameters, k)
        #         end
        #     end
        # end

        # Append to form a compositions of PTs that is twic in length
        append!(pt_stack.pts, second_half)
    end

    # Duplicate annotation-level constraints for Co, Equal, Cross, AtMost, ScaleBy
    pa.constraints = duplicated_constraints_for_batching(pa.constraints, pa)
    # println(pa.constraints)

    # Add constraints for second half being Sequential and Match-ing the first
    # half
    for (v, pt_stack) in pa.partitions.pt_stacks
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
            # push!(
            #     pa.constraints.constraints,
            #     PartitioningConstraintOverGroup("MATCH", [(v, i - 1), (v, dupi - 1)]),
            # )

            # Duplicate PT-level constraints
            pt_stack.pts[dupi].constraints =
                duplicated_constraints_for_batching(pt_stack.pts[dupi].constraints, pa)
        end
    end
end
