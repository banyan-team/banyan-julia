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

function partitioned_using(handler::Function)
    global curr_delayed_task
    curr_delayed_task.partitioned_using_func = handler
end

# TODO: Switch key names from strings to symbols if performance is an issue

function keep_all_sample_keys(participants::AbstractFuture...; drifted = false)
    # Take the union of discovered grouping keys. Grouping keys are discovered
    # through calls to `keep_keys` in functions where we know that we need
    # to allow grouping by certain keys such as sorting and join functions.
    groupingkeys = union([sample(p, :groupingkeys) for p in participants]...)
    if !drifted
        statistics = merge([sample(p, :statistics) for p in participants]...)
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
            setsample!(
                new,
                :groupingkeys,
                union(sample(new, :groupingkeys), [new_key]),
            )
            setsample!(
                new,
                :statistics,
                new_key,
                sample(old, :statistics, old_key),
            )
        end
        if new_key in sample(new, :groupingkeys)
            setsample!(
                old,
                :groupingkeys,
                union(sample(old, :groupingkeys), [old_key]),
            )
            setsample!(
                old,
                :statistics,
                old_key,
                sample(new, :statistics, new_key),
            )
        end
    end
end

function keep_sample_keys_named(
    participants::Pair{AbstractFuture,Any}...;
    drifted = false,
)
    # `participants` maps from futures to lists of key names such that all
    # participating futures have the same sample properties for the keys at
    # same indices in those lists
    participants = [
        (participant, Symbol.(to_vector(key_names))) for
        (participant, key_names) in participants
    ]
    nkeys = length(last(first(participants)))
    for i = 1:nkeys
        # Copy over allowed grouping keys
        for (p, keys) in participants
            p_key = keys[i]
            setsample!(
                p,
                :groupingkeys,
                union(sample(p, :groupingkeys), [p_key]),
            )
        end

        # Copy over statistics if they haven't changed
        if !drifted
            key_statistics = merge(
                [
                    sample(p, :statistics, keys[i]) for
                    (p, keys) in participants
                ]...,
            )
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
    keep_sample_keys_named([p => keys for p in participants]...)
end

# This is useful for workloads that involve joins where the sample rate is
# diminished quadratically for each joinv
keep_sample_rate(fut::AbstractFuture, relative_to::AbstractFuture...) =
    setsample!(fut, :rate, prod([sample(r, :rate) for r in relative_to]))

###############################
# Using samples to assign PTs #
###############################

function partitioned_with(handler::Function)
    # NOTE: `partitioned_with` should mainly include calls to `partition`;
    # calls to `mut` should be made outside
    global curr_delayed_task
    curr_delayed_task.partitioned_with_func = handler
end

function pt(
    args::Union{
        AbstractFuture,
        PartitionType,
        PartitionTypeComposition,
        Vector,
    }...;
    kwargs...,
)
    pa = get_pa()

    # Extract PT and args to assign the PT to from given arguments
    futs, ptype = args[1:end-1], last(args)

    if length(futs) > 1 && ptype isa Vector
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

        # Put in PT
        if ptype isa Vector
            for pty in ptype
                pt(fut, PartitionTypeComposition([pty]); kwargs...)
            end
        elseif ptype isa PartitionType
            pt(fut, PartitionTypeComposition([ptype]); kwargs...)
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
                    push!(
                        pa.constraints.constraints,
                        Match(fut, to_match_with...),
                    )
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
mutated(ff::Pair{<:AbstractFuture,<:AbstractFuture}) =
    mutated(first(ff), last(ff))

function mutated(old::AbstractFuture, new::AbstractFuture)
    global curr_delayed_task
    curr_delayed_task.mutation[convert(Future, old)] = convert(Future, new)
end

#################################################
# Macro for wrapping the code region to offload #
#################################################

function apply_mutation(mutation::Dict{Future,Future})
    for (old, new) in mutation
        if old != new
            # Swap references in `futures_on_client` if either side of the
            # mutation is on the client
            futures_on_client = get_job().futures_on_client
            if old.value_id in keys(futures_on_client) &&
               new.value_id in keys(futures_on_client)
                futures_on_client[new.value_id],
                futures_on_client[old.value_id] =
                    futures_on_client[old.value_id],
                    futures_on_client[new.value_id]
            elseif old.value_id in keys(futures_on_client)
                futures_on_client[new.value_id] =
                    futures_on_client[old.value_id]
                delete!(futures_on_client, old.value_id)
            elseif new.value_id in keys(futures_on_client)
                futures_on_client[old.value_id] =
                    futures_on_client[new.value_id]
                delete!(futures_on_client, new.value_id)
            end

            # Swap other fields of the `Future`s and their locations
            old.value,
            old.value_id,
            old.mutated,
            old.stale,
            get_job().locations[old.value_id],
            new.value,
            new.value_id,
            new.mutated,
            new.stale,
            get_job().locations[new.value_id] = new.value,
            new.value_id,
            new.mutated,
            new.stale,
            get_job().locations[new.value_id],
            old.value,
            old.value_id,
            old.mutated,
            old.stale,
            get_job().locations[old.value_id]
        end
    end
end

invert(mutation::Dict{Future,Future}) =
    Dict(new => old for (old, new) in mutation)

macro partitioned(ex...)
    res = quote end

    # Load in variables and code from macro
    variables = [esc(e) for e in ex[1:end-1]]
    variable_names = [string(e) for e in ex[1:end-1]]
    code = ex[end]

    # # Expand splatted variables
    # # if any(endswith(name, "...") for name in variable_names)
    # if true
    #     new_variables = []
    #     new_variable_names = []

    #     # Iterate through variables specified through the annotation
    #     for (variable, name) in zip(variables, variable_names)
    #         splatted = Meta.parse(name)
    #         println(__module__.eval(quote $variable end))
    #         # println(eval(quote $(variable) end))
    #     end

    #     # Iterate through variables specified through the annotation
    #     for (variable, name) in zip(variables, variable_names)
    #         # Variables with names ending with ... need to be expanded
    #         if endswith(name, "...")
    #             # Get information about the variable being splatted
    #             expanded_expr = Meta.parse(name[1:end-3])
    #             println(typeof(expanded_expr))
    #             println(:([$(esc(ex[2]))]))
    #             println(Base.eval(__module__, :([$(esc(ex[2]))])))
    #             expanded::Vector{AbstractFuture} = Base.eval(__module__, esc(expanded_expr))

    #             # Append to the variables and variable names to be loaded
    #             # into the string
    #             expanded_variables = []
    #             for (i, e) in enumerate(expanded)
    #                 # Appemnd to the variables and variable names to be used.
    #                 # We use a randomly-suffixed name for each variable in the
    #                 # expansion.
    #                 new_variable_name = name[1:end-3] * "_" * randstring(4)
    #                 new_variable = Meta.parse(new_variable_name)
    #                 push!(new_variable_names, new_variable_name)
    #                 push!(expanded_variables, new_variable)

    #                 # Then, in the code that this macro compiles to, we
    #                 # construct these randomly-suffixed variables to reference
    #                 # the appropriate future.
    #                 res = quote
    #                     $res
    #                     $new_variable = $expanded_expr[$i]
    #                 end
    #             end
    #             append!(new_variables, expanded_variables)

    #             # Append to the code so that the variable can be used as-is in
    #             # the code regiony
    #             code = quote
    #                 $expanded_expr = [$(expanded_variables...)]
    #                 $code
    #             end
    #         else
    #             push!(new_variables, variable)
    #             push!(new_variable_names, name)
    #         end
    #     end
    #     variables = new_variables
    #     variables_names = new_variable_names
    # end

    # Assign samples to variables used in annotated code
    assigning_samples = [
        quote
            unsplatted_future = unsplatted_futures[$i]
            # println(get_job().locations)
            # println(typeof(unsplatted_future))
            # unsplatted_variable_names = [$(variable_names...)]
            # println(unsplatted_variable_names[$i])
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
        # [unsplatted_future isa Base.Vector ? convert.(Future, unsplatted_future : convert(Future))]
        # for unsplatted_future in unsplatted_futures
        #     println(unsplatted_future)
        #     println(typeof(unsplatted_future))
        # end
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
        # splatted_futures::Vector{Future} = [
        #     (
        #         unsplatted_future isa Tuple ?
        #         Tuple([convert(Future, uf) for uf in unsplatted_future]) :
        #         convert(Future, unsplatted_future)
        #     ) for unsplatted_future in unsplatted_futures
        # ]
        # splatted_futures::Vector{AbstractFuture} = vcat(unsplatted_futures...) .|> x->convert.(Future,x)

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
        unsplatted_variable_names = [$(variable_names...)]
        splatted_variable_names = []
        task = get_task()
        # Get code to initialize the unsplatted variable in the code region
        task.code = ""
        for (variable, unsplatted_variable_name) in
            zip(unsplatted_futures, unsplatted_variable_names)
            task.code *= "$variable = "
            if variable isa Tuple
                task.code *= "["
                for (i, v) in enumerate(variable)
                    push!(
                        splatted_variable_names,
                        unsplatted_variable_name * "_$i",
                    )
                    task.code *= "$unsplatted_variable_name, "
                end
                task.code *= "]\n"
            else
                push!(splatted_variable_names, unsplatted_variable_name)
                task.code *= "$unsplatted_variable_name\n"
            end
        end
        task.code *= $(string(code))
        task.value_names = Dict(
            fut.value_id => var_name for (fut, var_name) in
            # zip(futures, [$(variable_names...)])
            zip(splatted_futures, splatted_variable_names)
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
        for fut in splatted_futures
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
        # begin
        $(assigning_samples...)
        # y=10
        # x= (5,6,7,8,y,y)
        # # Store samples in variables
        # # let $(variables...) = [f isa Vector ? sample.(f) : sample(f) for f in unsplatted_futures]
        # # TODO: Generate code ro store smaples in varibles
        # samples = [(f isa Vector ? sample.(f) : sample(f)) for f in unsplatted_futures]
        # ($(variables...)) = samples

        # Run the actual code. We don't have to do any splatting here
        # because the variables already each contain either a single
        # future or a list of them.
        $(esc(code))

        # Move results from variables back into the samples
        for (f, value) in zip(unsplatted_futures, [$(variables...)])
            if f isa Tuple
                for (fe, ve) in zip(f, value)
                    setsample!(fe, ve)
                end
            else
                setsample!(f, value)
            end
        end
        # end
        # end

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
    [
        (v, idx + div(length(pa.partitions.pt_stacks[v].pts), 2)) for
        (v, idx) in args
    ]
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
                elseif c.type == "CO_GROUP" &&
                       any((v, i - 1) in group for group in c.args)
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
        push!(
            pa.constraints.constraints,
            PartitioningConstraintOverGroup("CO", co_args),
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

duplicated_constraints_for_batching(
    pc::PartitioningConstraints,
    pa::PartitionAnnotation,
) = begin
    # promote(
    #     Union{PartitioningConstraint, Function}[],
    new_pts = vcat(
        [
            if c.type == "CO" || c.type == "EQUAL" || c.type == "SEQUENTIAL"
                [
                    deepcopy(c),
                    PartitioningConstraintOverGroup(
                        c.type,
                        duplicate_args(c.args, pa),
                    ),
                ]
            elseif c.type == "CROSS" ||
                   startswith(c.type, "AT_MOST=") ||
                   c.type == "MATCH" ||
                   startswith(c.type, "MATCH_ON")
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
            end for c in pa.constraints.constraints
        ]...,
    )
    # println()
    # println(new_pts)
    # )
    PartitioningConstraints(new_pts)
end

function duplicate_for_batching!(pa::PartitionAnnotation)
    # Duplicate annotation-level constraints for Co, Equal, Cross, AtMost, ScaleBy
    pa.constraints = duplicated_constraints_for_batching(pa.constraints, pa)
    # println(pa.constraints)

    # Duplicate PT stacks with second half being Sequential and Match-ing the
    # first half
    for (v, pt_stack) in pa.partitions.pt_stacks
        append!(pt_stack.pts, deepcopy(pt_stack.pts))
        for i = 1:div(length(pt_stack.pts), 2)
            dupi = i + div(length(pt_stack.pts), 2)

            # Add in `Sequential` and `Match` constraints for the duplicated
            # part of the PT composition
            push!(
                pa.constraints.constraints,
                PartitioningConstraintOverGroup("SEQUENTIAL", [(v, dupi - 1)]),
            )
            push!(
                pa.constraints.constraints,
                PartitioningConstraintOverGroup(
                    "MATCH",
                    [(v, i - 1), (v, dupi - 1)],
                ),
            )

            # Duplicate PT-level constraints
            pt_stack.pts[dupi].constraints =
                duplicated_constraints_for_batching(
                    pt_stack.pts[dupi].constraints,
                    pa,
                )
        end
    end
end
