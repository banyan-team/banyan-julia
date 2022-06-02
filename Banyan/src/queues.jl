#################
# GET QUEUE URL #
#################

get_sqs_dict_from_url(url::String)::Dict{Symbol,Any} =
    merge(
        get_aws_config(),
        Dict(:resource => "/" * joinpath(splitpath(url)[end-1:end]))
    )

get_scatter_queue()::Dict{Symbol,Any} =
    get_sqs_dict_from_url(get_session().scatter_queue_url)

get_gather_queue()::Dict{Symbol,Any} =
    get_sqs_dict_from_url(get_session().gather_queue_url)

get_execution_queue()::Dict{Symbol,Any} =
    get_sqs_dict_from_url(get_session().execution_queue_url)

###################
# RECEIVE MESSAGE #
###################

function sqs_receive_message_with_long_polling(queue)
    r = AWSSQS.sqs(queue, "ReceiveMessage", MaxNumberOfMessages = "1")
    r = r["messages"]

    if isnothing(r)
        return nothing
    end

    handle  = r[1]["ReceiptHandle"]
    id      = r[1]["MessageId"]
    message = r[1]["Body"]
    md5     = r[1]["MD5OfBody"]

    Dict{Symbol,Any}(
        :message => message,
        :id => id,
        :handle => handle
    )
end

function get_next_message(
    queue,
    p::Union{Nothing,ProgressMeter.ProgressUnknown} = nothing;
    delete::Bool = true,
    error_for_main_stuck::Union{Nothing,String} = nothing,
    error_for_main_stuck_time::Union{Nothing,DateTime} = nothing
)::Tuple{String,Union{Nothing,String}}
    m = sqs_receive_message_with_long_polling(queue)
    i = 1
    j = 1
    while (isnothing(m))
        error_for_main_stuck = check_worker_stuck(error_for_main_stuck, error_for_main_stuck_time)
        m = sqs_receive_message_with_long_polling(queue)
        @show m
        i += 1
        @show i
        if !isnothing(p)
            p::ProgressMeter.ProgressUnknown
            next!(p)
            j += 1
            @show j
        end
    end
    if delete
        sqs_delete_message(queue, m)
    end
    return m[:message]::String, error_for_main_stuck
end

function receive_next_message(
    queue_name,
    p=nothing,
    error_for_main_stuck=nothing,
    error_for_main_stuck_time=nothing
)::Tuple{Dict{String,Any},Union{Nothing,String}}
    content::String, error_for_main_stuck::Union{Nothing,String} = get_next_message(queue_name, p; error_for_main_stuck=error_for_main_stuck, error_for_main_stuck_time=error_for_main_stuck_time)
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
    send_message(
        get_gather_queue(),
        JSON.json(message)
    )
    # Receive response from client
    m = JSON.parse(get_next_message(get_scatter_queue())[1])
    v = from_jl_value_contents(m["contents"]::String)
    v
end


################
# SEND MESSAGE #
################

function send_message(queue_name, message)
    generated_message_id = generate_message_id()
    @show generated_message_id
    sqs_send_message(
        queue_name,
        message,
        (:MessageGroupId, "1"),
        (:MessageDeduplicationId, generated_message_id),
    )
end

function send_to_client(value_id::ValueId, value, worker_memory_used = 0)
    MAX_MESSAGE_LENGTH = 220_000
    message = to_jl_value_contents(value)::String
    i = 1
    while true
        is_last_message = length(message) <= MAX_MESSAGE_LENGTH
        @show i
        send_message(
            get_gather_queue(),
            JSON.json(
                Dict{String,Any}(
                    "kind" => (is_last_message ? "GATHER_END" : "GATHER"),
                    "value_id" => value_id,
                    "contents" => if is_last_message
                        message
                    else
                        msg = message[1:MAX_MESSAGE_LENGTH]
                        message = message[MAX_MESSAGE_LENGTH+1:end]
                        msg
                    end,
                    "worker_memory_used" => worker_memory_used,
                    "gather_page_idx" => i
                )
            )
        )
        i += 1
        @show is_last_message, length(message), MAX_MESSAGE_LENGTH
        if is_last_message
            break
        end
    end
end
