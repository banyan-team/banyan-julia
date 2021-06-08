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
    m = sqs_receive_message(queue_name)
    while isnothing(m)
        m = sqs_receive_message(queue_name)
    end
    content = m[:message]
    sqs_delete_message(queue_name, m)
    if startswith(content, "EVALUATION_END")
        @debug "Received evaluation end"
        println(content[15:end])
        Dict("kind" => "EVALUATION_END")
    elseif startswith(content, "JOB_FAILURE")
        @debug "Job failed"
        global current_job_status
        current_job_status = "failed"
        println(content[12:end])
        error("Job failed; see preceding output")
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
