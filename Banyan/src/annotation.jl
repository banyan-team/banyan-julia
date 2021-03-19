# TODO: Support multi-threaded usage by storing a global array with an instance
# of this annotation state for each thread
global curr_pa_union = nothing
global curr_pa = nothing
global curr_mut = nothing

function reset_pa()
    global curr_pa
    curr_pa = PartitionAnnotation(Partitions(Dict()), PartitioningConstraints([]))
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
    pa::PartitionAnnotation
)::Vector{PartitionTypeReference}
    [
        (v, idx + div(length(pa.partitions.pt_stacks[v]), 2))
        for (v, idx) in args
    ]
end

function apply_default_constraints!(pa::PartitionAnnotation)
    # Add Cross constraints for all unconstrained PTs
    for (v, pt_stack) in pa.partitions.pt_stacks
        for i in 1:length(pt_stack)
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
                    PartitioningConstraint("CROSS", [(v, i-1)])
                )
            end
        end
    end

    # Add Co constraint for all Cross-ed PTs
    # TODO: Only add if there is at least one Cross
    co_group_args = [c.args for c in pa.constraints.constraints if c.type == "CROSS"]
    if length(co_group_args) > 0
        push!(
            pa.constraints.constraints,
            PartitioningConstraint("CO_GROUP", co_group_args)
        )
    end
end

function duplicate_for_batching!(pa::PartitionAnnotation)
    # Duplicate PT stacks with second half being Sequential and Match-ing the
    # first half
    for (v, pt_stack) in pa.partitions.pt_stacks
        append!(pt_stack, copy(pt_stack))
        for i in 1:div(length(pt_stack), 2)
            dupi = i + div(length(pt_stack), 2)
            push!(
                pa.constraints.constraints,
                PartitioningConstraint("SEQUENTIAL", [(v, dupi - 1)])
            )
            push!(
                pa.constraints.constraints,
                PartitioningConstraint("MATCH", [(v, i - 1), (v, dupi - 1)])
            )
        end
    end

    # Duplicate constraints for Co, Equal, Cross, AtMost
    new_constraints = []
    for c in pa.constraints.constraints
        if c.type == "CO" || c.type == "EQUAL"
            push!(
                new_constraints,
                PartitioningConstraint(c.type, duplicate_args(c.args, pa))
            )
        elseif c.type == "CROSS" || startswith(c.type, "AT_MOST")
            append!(c.args, duplicate_args(c.args, pa))
        elseif c.type == "CO_GROUP"
            for group in c.args
                append!(group, duplicate_args(group, pa))
            end
        end
    end 
    append!(pa.constraints.constraints, new_constraints)
end

function add_pa_to_union()
    global curr_pa_union
    global curr_pa
    # TODO: Ensure this actually copies over the PA and doesn't just
    # copy over a reference that then gets reset
    apply_default_constraints!(curr_pa)
    duplicate_for_batching!(curr_pa)
    push!(curr_pa_union, curr_pa)
    reset_pa()
end

function get_mutated(v::ValueId)
    global curr_mut
    return v in curr_mut
end

function get_pa_union()
    global curr_pa_union
    return curr_pa_union
end

function pt(fut, pt::PartitionTypeComposition)
    global curr_pa_union
    global curr_pa
    fut = future(fut)
    if fut.value_id in keys(curr_pa.partitions.pt_stacks)
        add_pa_to_union()
    end
    curr_pa.partitions.pt_stacks[fut.value_id] = pt_composition_from_pts(pt)
end

# TODO: Implement PT transformations
function pt(
    fut,
    pre_pt::PartitionTypeComposition,
    post_pt::PartitionTypeComposition,
) end

function pc(constraint::PartitioningConstraint)
    global curr_pa_union
    global curr_pa
    push!(curr_pa.constraints.constraints, constraint)
end

function mut(fut)
    global curr_mut
    push!(curr_mut, future(fut).value_id)
end

macro partitioned(ex...)
    variables = [esc(e) for e in ex[1:end-1]]
    variable_names = [string(e) for e in ex[1:end-1]]
    code = ex[end]

    return quote
        # Create task
        add_pa_to_union()
        task = BTask(
        	$(string(code)),
        	Dict(
                future(fut).value_id => var_name
                for (fut, var_name) in zip([$(variables...)], [$(variable_names...)])
            ),
        	Dict(
                future(fut).value_id => if get_mutated(future(fut).value_id) "MUT" else "CONST" end
                for fut in [$(variables...)]
            ),
            get_pa_union()
        )

        # Set mutated
        for fut in [$(variables...)]
            if get_mutated(future(fut).value_id)
                future(fut).mutated = true
            end
        end

        # Record request to record task in backend's dependency graph and reset
        record_request(RecordTaskRequest(task))
        reset_annotation()
    end
end