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
    message = JSON.parse(m[:message])
    sqs_delete_message(queue_name, m)
    println(message)
    return message
end

function send_message(queue_name, message)
    sqs_send_message(
        queue_name,
        message,
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, get_message_id()),
    )
end
