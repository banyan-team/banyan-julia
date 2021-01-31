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

function add_pa_to_union()
    global curr_pa_union
    global curr_pa
    # TODO: Ensure this actually copies over the PA and doesn't just
    # copy over a reference that then gets reset
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
    curr_pa.partitions.pt_stacks[fut.value_id] = pt
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