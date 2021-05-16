###############################
# Global state for annotation #
###############################

curr_delayed_task = DelayedTask()

function set_task(t::DelayedTask)
    global curr_delayed_task
    curr_delayed_task = t
end

function get_task()
    global curr_delayed_task
    curr_delayed_task
end

function finish_task(t::DelayedTask)
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

function partitioned_using(handler::Function)
    global curr_delayed_task
    curr_delayed_task.partitioned_using_func = handler
end

# TODO: Switch key names from strings to symbols if performance is an issue

function keep_all_keys(participants::AbstractFuture...)
    # Take the union of discovered grouping keys. Grouping keys are discovered
    # through calls to `keep_keys` in functions where we know that we need
    # to allow grouping by certain keys such as sorting and join functions.
    groupingkeys = union([sample(p, :groupingkeys) for p in participants]...)
    statistics = merge([sample(p, :statistics) for p in participants]...)

    # Only allow keys that are actually in the keys of the participants
    # TODO: Maybe replace this with a function that doesn't require calling
    # a potentially expensive function to iterate through _all_ columns.
    for p in participants
        p_keys = intersect(groupingkeys, sample(p, :keys))
        setsample!(p, :groupingkeys, p_keys)
        setsample!(
            p,
            :statistics,
            Dict(
                key => statistic
                for (key, statistic) in statistics
                if key in sample(p, :keys)
            )
        )
    end
end

function keep_all_keys_renamed(old::AbstractFuture, new::AbstractFuture)
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

function keep_keys_renamed(participants::Pair{AbstractFuture, Any}...)
    nkeys = length(last(first(participants)))
    for i in 1:nkeys
        key_statistics = merge([sample(p, :statistics, keys[i]) for (p, keys) in participants]...)
        for (p, keys) in participants
            p_key = keys[i]
            setsample!(p, :groupingkeys, union(sample(p, :groupingkeys), [p_key]))
            setsample!(p, :statistics, key_statistics)
        end
    end
end

keep_keys(keys, participants::AbstractFuture...) =
    keep_keys_renamed([p => keys for p in participants]...)

function memory_usage_relative_to(fut::AbstractFuture, relative_to::AbstractFuture...)
    # This is useful for workloads that involve joins where the sample rate is
    # diminished quadratically for each join
    setsample!(fut, :rate, prod([sample(r, :rate) for r in relative_to]))
end

###############################
# Using samples to assign PTs #
###############################

function partitioned_with(handler::Function)
    # NOTE: `partitioned_with` should mainly include calls to `partition`;
    # calls to `mut` should be made outside
    global curr_delayed_task
    curr_delayed_task.partitioned_with_func = handler
end

function pt(args::Union{AbstractFuture,PartitionType,PTComposition,PTUnion}...; kwargs...)
    pa = get_pa()
    
    # Extract PT and args to assign the PT to from given arguments
    futs, ptype = args[1:end-1], last(args)

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

        # Put in PT
        if ptype isa PTUnion
            for pty in ptype.pts
                pt(fut, PTComposition([pty]); kwargs...)
            end
        elseif ptype isa PartitionType
            pt(fut, PTComposition([ptype]); kwargs...)
        elseif ptype isa PTComposition
            # Add to PAs information about how partitions are produced
            pa.partitions.pt_stacks[fut.value_id] = ptype

            # Add to PAs information about how partitioning is constrained
            for c in get(kwargs, :constrain, [])
                push!(pa.constraints.constraints, c)
            end

            # Handle `match`, `on` in keyword arguments
            if :match in keys(kwargs)
                to_match_with = to_vector(kwargs[:match_with])
                if :on in keys(kwargs)
                    for to_match_on in to_vector(get(kwargs, :on, []))
                        push!(
                            pa.constraints.constraints,
                            MatchOn([fut; to_match_with; to_match_on]...)
                        )
                    end
                else
                    push!(
                        pa.constraints.constraints,
                        Match([fut; to_match_with]...)
                    )
                end
            end
            
            # TODO: Implement support for other constraints in kwargs

            # Handle `mut` in kwargs
            if :mut in keys(kwargs) && kwargs[:mut]
                mut(fut)
            end
        else
            throw(ArgumentError("Expected partition type (PT) or a composition or union of PTs"))
        end
    end
end

mut(f::AbstractFuture) = mut(f => f)
mut(ff::Pair{AbstractFuture, AbstractFuture}) = mut(first(ff), last(ff))

function mut(old::AbstractFuture, new::AbstractFuture)
    global curr_delayed_task
    curr_delayed_task.mutation[convert(Future, old)] = convert(Future, new)
end

#################################################
# Macro for wrapping the code region to offload #
#################################################

function apply_mutation(mutation::Dict{Future, Future})
    for (old, new) in task.mutation
        if old != new
            # Swap references in `futures_on_client` if either side of the
            # mutation is on the client
            futures_on_client = get_job().futures_on_client
            if old.value_id in keys(futures_on_client) && new.value_id in keys(futures_on_client)
                futures_on_client[new.value_id], futures_on_client[old.value_id] = 
                    futures_on_client[old.value_id], futures_on_client[new.value_id]
            elseif old.value_id in keys(futures_on_client)
                futures_on_client[new.value_id] = futures_on_client[old.value_id]
                delete!(futures_on_client, old.value_id)
            elseif new.value_id in keys(futures_on_client)
                futures_on_client[old.value_id] = futures_on_client[new.value_id]
                delete!(futures_on_client, new.value_id)
            end

            # Swap other fields of the `Future`s and their locations
            old.value, old.value_id, old.mutated, old.stale, get_job().locations[old.value_id],
            new.value, old.value_id, old.mutated, old.stale, get_job().locations[old.value_id] =
                new.value, old.value_id, old.mutated, old.stale, get_job().locations[old.value_id],
                old.value, old.value_id, old.mutated, old.stale, get_job().locations[old.value_id]
        end
    end
end

invert(mutation::Dict{Future,Future}) = Dict(new => old for (old, new) in mutation)

macro partitioned(ex...)
    variables = [esc(e) for e in ex[1:end-1]]
    variable_names = [string(e) for e in ex[1:end-1]]
    code = ex[end]

    return quote
        # Convert arguments to `Future`s if they aren't already
        futures::Vector{Future} = [$(variables...)] .|> x->convert(Future,x)

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

        # Fill in task with code and value names pulled using the macro
        task = get_task()
        task.code = $(string(code))
        task.value_names = Dict(
            fut.value_id => var_name for (fut, var_name) in
            zip(futures, [$(variable_names...)])
        )
        # task = DelayedTask(
        #     ,
        #     ,
        #     # Dict(
        #     #     fut.value_id =>
        #     #         if get_mutated(fut.value_id)
        #     #             "MUT"
        #     #         else
        #     #             "CONST"
        #     #         end for fut in futures
        #     # ),
        #     Dict(),
        #     get_pa_union(),
        # )

        # Set `mutated` field of the `Future`s that have been mutated. This is
        # to ensure that future calls to `evaluate` on those `Future`s with
        # `mutated=true` and _only_ those `Future`s will result in an actual
        # evaluation
        for fut in futures
            if fut in values(task.mutation)
                fut.stale = true
                fut.mutated = true
                task.effects[fut.value_id] = "MUT"
            else
                task.effects[fut.value_id] = "CONST"
            end
        end

        # Record request to record task in backend's dependency graph and reset
        record_request(RecordTaskRequest(task))
        finish_task()

        # Perform computation on samples
        begin
            futures = [$(variables...)]
            $(variables...) = [sample(f) for f in futures]
            $code
            for (f, value) in zip(futures, [$(variables...)])
                setsample!(f, value)
            end
        end

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
    [
        (v, idx + div(length(pa.partitions.pt_stacks[v]), 2)) for
        (v, idx) in args
    ]
end

function apply_default_constraints!(pa::PartitionAnnotation)
    # Add Cross constraints for all unconstrained PTs
    for (v, pt_stack) in pa.partitions.pt_stacks
        for i = 1:length(pt_stack)
            # Check if in_cross_or_co
            in_cross_or_co = false
            for c in pa.constraints.constraints
                if (c.type == "CROSS" || c.type == "CO") &&
                   (v, i - 1) in c.args
                    in_cross_or_co = true
                elseif c.type == "CO_GROUP" &&
                       any((v, i - 1) in group for group in c.args)
                    in_cross_or_co = true
                end
            end

            # Add Cross constraint for those not constrained in any way
            if !in_cross_or_co
                push!(
                    pa.constraints.constraints,
                    PartitioningConstraint("CROSS", [(v, i - 1)]),
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
        push!(
            pa.constraints.constraints,
            PartitioningConstraint("CO", co_args),
        )
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

function duplicate_for_batching!(pa::PartitionAnnotation)
    # Duplicate PT stacks with second half being Sequential and Match-ing the
    # first half
    for (v, pt_stack) in pa.partitions.pt_stacks
        append!(pt_stack, deepcopy(pt_stack))
        for i = 1:div(length(pt_stack), 2)
            dupi = i + div(length(pt_stack), 2)
            push!(
                pa.constraints.constraints,
                PartitioningConstraint("SEQUENTIAL", [(v, dupi - 1)]),
            )
            push!(
                pa.constraints.constraints,
                PartitioningConstraint("MATCH", [(v, i - 1), (v, dupi - 1)]),
            )
        end
    end

    # Duplicate constraints for Co, Equal, Cross, AtMost
    new_constraints = []
    for c in pa.constraints.constraints
        if c.type == "CO" || c.type == "EQUAL"
            push!(
                new_constraints,
                PartitioningConstraint(c.type, duplicate_args(c.args, pa)),
            )
        elseif c.type == "CROSS" || startswith(c.type, "AT_MOST")
            append!(c.args, duplicate_args(c.args, pa))
        elseif c.type == "CO_GROUP"
            push!(
                new_constraints,
                PartitioningConstraint(c.type, [duplicate_args(group, pa) for group in c.args]),
            )
            # for group in c.args
            #     append!(group, duplicate_args(group, pa))
            # end
        end
    end
    append!(pa.constraints.constraints, new_constraints)
end

######################################################################
# Helper functions for building union of PAs for current code region #
######################################################################

# TODO: Support multi-threaded usage by storing a global array with an instance
# of this annotation state for each thread
global curr_pa_union = nothing
global curr_pa = nothing
global curr_mut = nothing

function reset_pa()
    global curr_pa
    curr_pa =
        PartitionAnnotation(Partitions(Dict()), PartitioningConstraints([]))
end

function reset_annotation()
    global curr_pa_union
    global curr_mut
    reset_pa()
    curr_pa_union = []
    curr_mut = []
end

reset_annotation()

function get_mutated(v::ValueId)::Bool
    global curr_mut
    return v in curr_mut
end

function get_pa_union()::Vector{PartitionAnnotation}
    global curr_pa_union
    return curr_pa_union
end

function get_pa()::PartitionAnnotation
    global curr_pa
    return curr_pa
end

function add_pa_to_union()
    curr_pa = get_pa()
    # TODO: Ensure this actually copies over the PA and doesn't just
    # copy over a reference that then gets reset
    push!(get_pa_union(), curr_pa)
    reset_pa()
end

#################################################################
# Fundamental functions in the Partition Annotation abstraction #
#################################################################

function pt(fut, pt::Delayed{PartitionTypeComposition})
    curr_pa = get_pa()
    fut = future(fut)
    if fut.value_id in keys(curr_pa.partitions.pt_stacks)
        add_pa_to_union()
    end
    curr_pa.partitions.pt_stacks[fut.value_id] = pt
end

# TODO: Implement PT reinterpretation

# function reinterpret(fut)
#     # Wee store newfut for generating assignment of newfut to fut in code
#     # region and fut to nothing. We also set location of newfut to that of fut
#     # but change its src to be None. The user is expected to add PCs to ensure
#     # newfut and fut match or are co-partitioned as needed. The user is also
#     # expected to ensure that fut is GC-able after this code region and newfut
#     # is being used instead

#     global curr_pa

#     # Create reinterpreted future with location
#     newfut = Future()
#     loc(newfut, fut)
#     src(newfut, None())

#     # Store for reinterpretation
#     curr_pa

#     newfut
# end

# function pt(
#     fut,
#     pre_pt::PartitionTypeComposition,
#     post_pt::PartitionTypeComposition,
# ) end

function pc(constraint::Delayed{PartitioningConstraint})
    push!(get_pa().constraints.constraints, constraint)
end

function mut(fut)
    global curr_mut
    push!(curr_mut, future(fut).value_id)
end

######################################
# High-level function for annotation #
######################################

# TODO: Implement high-level partition function
# function partition(
#     fut::AbstractFuture;
#     pts = Replicated(),
#     mutating = false,
#     cross_with = nothing,
#     match_with = nothing
# )
#     # Get future and assign PT
#     fut = convert(Future, fut)
#     pt(fut, pts)
#     if mutating
#         mut(fut)
#     end

#     # TODO: Apply constraints and locations
#     if !isnothing(cross_with)
#         pc(Cross(fut, cross_with))
#     end

#     # TODO: Add ability to specify sample, sample properties, total memory
#     # usage to copy from another value
# end

#################################################
# Macro for wrapping the code region to offload #
#################################################

macro partitioned(ex...)
    variables = [esc(e) for e in ex[1:end-1]]
    variable_names = [string(e) for e in ex[1:end-1]]
    code = ex[end]

    return quote
        # Convert arguments to `Future`s if they aren't already
        futures = [$(variables...)] .|> x->convert(Future,x)

        # Add last PA in union of PAs to the task being recorded here
        add_pa_to_union()

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

        # Create task
        task = DelayedTask(
            $(string(code)),
            Dict(
                fut.value_id => var_name for (fut, var_name) in
                zip(futures, [$(variable_names...)])
            ),
            Dict(
                fut.value_id =>
                    if get_mutated(fut.value_id)
                        "MUT"
                    else
                        "CONST"
                    end for fut in futures
            ),
            get_pa_union(),
        )

        # Set `mutated` field of the `Future`s that have been mutated. This is
        # to ensure that future calls to `evaluate` on those `Future`s with
        # `mutated=true` and _only_ those `Future`s will result in an actual
        # evaluation
        for fut in futures
            if get_mutated(fut.value_id)
                fut.stale = true
                fut.mutated = true
            end
        end

        # Record request to record task in backend's dependency graph and reset
        record_request(RecordTaskRequest(task))
        reset_annotation()

        # Lazily perform computation on samples of futures that are used
        begin
            futures = [$(variables...)]
            $(variables...) = [sample(f) for f in futures]
            $code
            for (f, value) in zip(futures, [$(variables...)])
                setsample!(f, value)
            end
        end
    end
end