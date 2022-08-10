#################
# GET QUEUE URL #
#################

scatter_queue_url()::String = get_session().scatter_queue_url
gather_queue_url()::String = get_session().gather_queue_url
execution_queue_url()::String = get_session().execution_queue_url

###################
# RECEIVE MESSAGE #
###################

function get_next_message(
    queue_url,
    p::Union{Nothing,ProgressMeter.ProgressUnknown} = nothing;
    delete::Bool = true,
    error_for_main_stuck::Union{Nothing,String} = nothing,
    error_for_main_stuck_time::Union{Nothing,DateTime} = nothing
)::Tuple{String,Union{Nothing,String}}
    error_for_main_stuck = check_worker_stuck(error_for_main_stuck, error_for_main_stuck_time)
    m = SQS.receive_message(queue_url, Dict("MaxNumberOfMessages" => "1"))
    i = 1
    j = 1
    while (!haskey(m, "ReceiveMessageResult") || !haskey(m["ReceiveMessageResult"], "Message"))
        error_for_main_stuck = check_worker_stuck(error_for_main_stuck, error_for_main_stuck_time)
        m = SQS.receive_message(queue_url, Dict("MaxNumberOfMessages" => "1"))
        i += 1
        if !isnothing(p)
            p::ProgressMeter.ProgressUnknown
            next!(p)
            j += 1
        end
    end
    m_dict = m["ReceiveMessageResult"]["Message"]
    if delete
        SQS.delete_message(queue_url, m_dict["ReceiptHandle"]::String)
    end
    return m_dict["Body"]::String, error_for_main_stuck
end

function sqs_receive_next_message(
    queue_name,
    p=nothing,
    error_for_main_stuck=nothing,
    error_for_main_stuck_time=nothing
)::Tuple{Dict{String,Any},Union{Nothing,String}}
    content::String, error_for_main_stuck::Union{Nothing,String} =
        get_next_message(queue_name, p; error_for_main_stuck=error_for_main_stuck, error_for_main_stuck_time=error_for_main_stuck_time)
    res::Dict{String,Any} = if startswith(content, "JOB_READY") || startswith(content, "SESSION_READY")
        Dict{String,Any}(
            "kind" => "SESSION_READY"
        )
    elseif startswith(content, "EVALUATION_END")
        # @debug "Received evaluation end"
        # Print out logs that were outputed by session on cluster. Will be empty if
        # `print_logs=false` for the session. Remove "EVALUATION_END" at start and
        #  chop off "MESSAGE_END" at the end
        if !isnothing(p) && !p.done
            finish!(p)
        end
        tail = endswith(content, "MESSAGE_END") ? 11 : 0
        print(chop(content, head=14, tail=tail))
        Dict{String,Any}(
            "kind" => "EVALUATION_END",
            "end" => endswith(content, "MESSAGE_END")
        )
    elseif startswith(content, "JOB_FAILURE") || startswith(content, "SESSION_FAILURE")
        if !isnothing(p) && !p.done
            finish!(p, spinner='✗')
        end
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
    res, error_for_main_stuck
end

# Used by Banyan/src/pfs.jl, intended to be called from the executor
function receive_from_client(value_id::ValueId)
    # Send scatter message to client
    message = Dict{String,String}("kind" => "SCATTER_REQUEST", "value_id" => value_id)
    sqs_send_message(gather_queue_url(), JSON.json(message))
    # Receive response from client
    m = JSON.parse(get_next_message(scatter_queue_url())[1])
    v = from_jl_string(m["contents"]::String)
    v
end


################
# SEND MESSAGE #
################

function sqs_send_message(queue_url, message)
    generated_message_id = generate_message_id()
    SQS.send_message(
        message,
        queue_url,
        Dict(
            "MessageGroupId" => "1",
            "MessageDeduplicationId" => generated_message_id
        )
    )
end

function send_to_client(value_id::ValueId, value, worker_memory_used = 0)
    MAX_MESSAGE_LENGTH = 220_000
    message = to_jl_string(value)::String
    generated_message_id = generate_message_id()

    # Break the message down into chunk ranges
    nmessages = 0
    message_length = length(message)
    message_ranges = []
    message_i = 1
    while true
        is_last_message = message_length <= MAX_MESSAGE_LENGTH
        starti = message_i
        if is_last_message
            message_i += message_length
            message_length = 0
        else
            message_i += MAX_MESSAGE_LENGTH
            message_length -= MAX_MESSAGE_LENGTH
        end
        push!(message_ranges, starti:(message_i-1))
        nmessages += 1
        if is_last_message
            break
        end
    end

    # Launch asynchronous threads to send SQS messages
    gather_q_url = gather_queue_url()
    num_chunks = length(message_ranges)
    @show num_chunks
    if num_chunks > 1
        @sync for i = 1:message_ranges
            @async begin
                msg = Dict{String,Any}(
                    "kind" => "GATHER",
                    "value_id" => value_id,
                    "contents" => message[message_ranges[i]],
                    "worker_memory_used" => worker_memory_used,
                    "chunk_idx" => i,
                    "num_chunks" => num_chunks
                )
                msg_json = JSON.json(msg)
                SQS.send_message(
                    msg_json,
                    gather_q_url,
                    Dict(
                        "MessageGroupId" => string(i),
                        "MessageDeduplicationId" => generated_message_id * string(i)
                    )
                )
                @show msg
                @show i
            end
        end
    else
        i = 1
        msg = Dict{String,Any}(
            "kind" => "GATHER",
            "value_id" => value_id,
            "contents" => message[message_ranges[i]],
            "worker_memory_used" => worker_memory_used,
            "chunk_idx" => i,
            "num_chunks" => num_chunks
        )
        @show msg
        msg_json = JSON.json(msg)
        SQS.send_message(
            msg_json,
            gather_q_url,
            Dict(
                "MessageGroupId" => string(i),
                "MessageDeduplicationId" => generated_message_id * string(i)
            )
        )
    end
end
