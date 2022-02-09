#################
# GET QUEUE URL #
#################



function get_scatter_queue(resource_id::Union{ResourceId,Nothing}=nothing)
    if isnothing(resource_id)
        resource_id = get_session().resource_id
    end
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", resource_id, "_scatter.fifo"),
    )
end

function get_gather_queue(resource_id::Union{ResourceId,Nothing}=nothing)
    if isnothing(resource_id)
        resource_id = get_session().resource_id
    end
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", resource_id, "_gather.fifo"),
    )
end

function get_execution_queue(resource_id::Union{ResourceId,Nothing}=nothing)
    if isnothing(resource_id)
        resource_id = get_session().resource_id
    end
    return sqs_get_queue_with_retries(
        get_aws_config(),
        string("banyan_", resource_id, "_execution.fifo"),
    )
end


###################
# RECEIVE MESSAGE #
###################

function get_next_message(queue, p=nothing; delete = true)
    m = sqs_receive_message(queue)
    while (isnothing(m))
        m = sqs_receive_message(queue)
        # @debug "Waiting for message from SQS"
        if !isnothing(p)
            next!(p)
        end
    end
    if delete
        sqs_delete_message(queue, m)
    end
    return m[:message]
end

function receive_next_message(queue_name, p=nothing)
    content = get_next_message(queue_name, p)
    if startswith(content, "JOB_READY") || startswith(content, "SESSION_READY")
        response = Dict{String,Any}(
            "kind" => "SESSION_READY"
        )
    elseif startswith(content, "EVALUATION_END")
        # @debug "Received evaluation end"
        response = Dict{String,Any}(
            "kind" => "EVALUATION_END",
            "end" => endswith(content, "MESSAGE_END")
        )
        # Print out logs that were outputed by session on cluster. Will be empty if
        # `print_logs=false` for the session. Remove "EVALUATION_END" at start and
        #  chop off "MESSAGE_END" at the end
        if !isnothing(p) && !p.done
            finish!(p)
        end
        tail = endswith(content, "MESSAGE_END") ? 11 : 0
        print(chop(content, head=14, tail=tail))
        response
    elseif startswith(content, "JOB_FAILURE") || startswith(content, "SESSION_FAILURE")
        if !isnothing(p) && !p.done
            finish!(p, spinner='✗')
        end
        # @debug "Session failed"
        # Print session logs. Will be empty if `print_logs=false` for the session. Remove
        # "JOB_FAILURE" and "JOB_END" from the message content. Note that logs
        # are streamed in multiple parts, due to SQS message limits.
        tail = endswith(content, "MESSAGE_END") ? 11 : 0
        head_len = startswith(content, "JOB_FAILURE") ? 11 : 15
        print(chop(content, head=head_len, tail=tail))
        # End session when last part of log is received.
        if endswith(content, "MESSAGE_END")
            # We have to end the session here because we could be receiving
            # this message as a result of the executor actually crashing. So a
            # new session entirely will have to be launched. Fortunately, the
            # provisioned nodes should stick around for a bit so it should
            # only be a couple of minutes before the session is back up and
            # running.
            end_session(failed=true, release_resources_now=startswith(content, "JOB_FAILURE")) # This will reset the `current_session_id` and delete from `sessions`
            error("Session failed; see preceding output")
        end
        Dict{String,Any}("kind" => "SESSION_FAILURE")
    else
        # @debug "Received scatter or gather request"
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









