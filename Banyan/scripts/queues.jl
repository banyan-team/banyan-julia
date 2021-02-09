using AWSCore
using AWSSQS
using JSON

AWS = aws_config(region = "us-west-2")

##################
# GET QUEUE URLS #
##################

function get_execution_queue()
    global job_id
    return sqs_get_queue(
        AWS,
        string("banyan_", job_id, "_execution.fifo"),
    )
end

function get_scatter_queue()
    global job_id
    return sqs_get_queue(
        AWS,
        string("banyan_", job_id, "_scatter.fifo"),
    )
end

function get_gather_queue()
    global job_id
    return sqs_get_queue(
        AWS,
        string("banyan_", job_id, "_gather.fifo"),
    )
end


###########################
# GET MESSAGES FROM QUEUE #
###########################

function get_next_message(queue; delete=true)
    m = sqs_receive_message(queue)
    while (isnothing(m))
        m = sqs_receive_message(queue)
    end
    if delete == true
        sqs_delete_message(queue, m)
    end
    message = JSON.parse(m[:message])
    return message
end

function get_next_execution_request()
    return get_next_message(get_execution_queue())
end


##########################
# SEND MESSAGES TO QUEUE #
##########################

message_id = 0
function get_message_id()
    global message_id
    message_id = message_id + 1
    return string(message_id)
end

function send_scatter_request(value_id)
    sqs_send_message(
        get_gather_queue(),
        JSON.json(Dict(
            "kind" => "SCATTER_REQUEST",
            "value_id" => value_id
        )),
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, get_message_id())
    )
end

function send_gather(value_id, value)
    sqs_send_message(
        get_gather_queue(),
        JSON.json(Dict(
            "kind" => "GATHER",
            "value_id" => value_id,
            "value" => value
        )),
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, get_message_id())
    )
end

function send_evaluation_end()
    sqs_send_message(
        get_gather_queue(),
        JSON.json(Dict{String,Any}("kind" => "EVALUATION_END")),
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, get_message_id()),
    )
end
