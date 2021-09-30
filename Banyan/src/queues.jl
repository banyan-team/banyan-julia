#using AWSCore
#using AWSSQS

function get_scatter_queue(job_id::JobId)
    return sqs_get_queue(
        get_aws_config(),
        string("banyan_", job_id, "_scatter.fifo"),
    )
end

function get_gather_queue(job_id::JobId)
    return sqs_get_queue(
        get_aws_config(),
        string("banyan_", job_id, "_gather.fifo"),
    )
end

function receive_next_message(queue_name)
    global jobs
    job_id = get_job_id()
    m = sqs_receive_message(queue_name)
    while isnothing(m)
        m = sqs_receive_message(queue_name)
        if is_debug_on()
            println("Waiting for message")
        end
    end
    content = m[:message]
    sqs_delete_message(queue_name, m)
    if startswith(content, "EVALUATION_END")
        @debug "Received evaluation end"
        # if is_debug_on()
            println(content[15:end])
        # end
        response = Dict{String,Any}("kind" => "EVALUATION_END")
        response["end"] = (endswith(content, "MESSAGE_END"))
        # TODO: Maybe truncate by chopping off the MESSAGE_END
        response
    elseif startswith(content, "JOB_FAILURE")
        @debug "Job failed"
        # jobs[job_id].current_status = "failed"
        set_job(nothing) # future usage of this job should immediately fail
        delete!(jobs, job_id)
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

function send_message(queue_name, message)
    sqs_send_message(
        queue_name,
        message,
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, generate_message_id()),
    )
end
