#################
# GET QUEUE URL #
#################



function get_scatter_queue(job_id::JobId=get_job_id())
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", job_id, "_scatter.fifo"),
    )
end

function get_gather_queue(job_id::JobId=get_job_id())
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", job_id, "_gather.fifo"),
    )
end

function get_execution_queue(job_id::JobId=get_job_id())
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", job_id, "_execution.fifo"),
    )
end


###################
# RECEIVE MESSAGE #
###################

function get_next_message(queue; delete = true)
    m = sqs_receive_message(queue)
    while (isnothing(m))
        m = sqs_receive_message(queue)
        if is_debug_on()
            println("Waiting for message")
        end
    end
    if delete
        sqs_delete_message(queue, m)
    end
    return m[:message]
end

function receive_next_message(queue_name)
    global jobs
    job_id = get_job_id()
    content = get_next_message(queue_name)
    if startswith(content, "EVALUATION_END")
        @debug "Received evaluation end"
        println(content[15:end])
        response = Dict{String,Any}("kind" => "EVALUATION_END")
        response["end"] = (endswith(content, "MESSAGE_END"))
        # TODO: Maybe truncate by chopping off the MESSAGE_END
        response
    elseif startswith(content, "JOB_FAILURE")
        @debug "Job failed"
        # jobs[job_id].current_status = "failed"
        # set_job(nothing) # future usage of this job should immediately fail
        # delete!(jobs, job_id)
        destroy_job(failed=true) # This will reset the `current_job_id` and delete from `jobs`
        # TODO: Document why the 12 here is necessary
        # if is_debug_on()
            println(content[12:end])
        # end
        if endswith(content, "MESSAGE_END")
            error("Job failed; see preceding output")
        end
        response = Dict{String,Any}("kind" => "JOB_FAILURE")
    else
        @debug "Received scatter or gather request"
        JSON.parse(content)
    end
end

# Used by Banyan/src/pfs.jl, intended to be called from the executor
function receive_from_client(value_id)
    # Send scatter message to client
    send_message(
        get_gather_queue(),
        JSON.json(Dict("kind" => "SCATTER_REQUEST", "value_id" => value_id))
    )
    # Receive response from client
    m = JSON.parse(get_next_message(get_scatter_queue()))
    v = from_jl_value_contents(m["contents"])
    v
end


################
# SEND MESSAGE #
################

function send_message(queue_name, message)
    sqs_send_message(
        queue_name,
        message,
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, generate_message_id()),
    )
end

function send_to_client(value_id, value)
    send_message(
        get_gather_queue(),
        JSON.json(
            Dict(
                "kind" => "GATHER",
                "value_id" => value_id,
                "contents" => to_jl_value_contents(value)
            )
        )
    )
end















###########################
# GET MESSAGES FROM QUEUE #
###########################




##########################
# SEND MESSAGES TO QUEUE #
##########################









