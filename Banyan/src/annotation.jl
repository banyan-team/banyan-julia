function partition_before()
end

function partition()
end

function partition()
end

function keep_all_keys()
    # Keeps all keys in original but only if in next
end

function keep_keys(keys)
end

function keep_keys_except(keys)
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
        task = BTask(
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