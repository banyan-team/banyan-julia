using Dates

@nospecialize

function run_with_retries(
    f,
    args...;
    retry_time = Dates.Second(32),
    failure_message = "Failed after retrying",
    kwargs...,
)
    # Exponential backoff algorithm recommended by Google Cloud:
    # https://cloud.google.com/iot/docs/how-tos/exponential-backoff
    time_of_failure = nothing
    nretries = 0
    while true
        q = f(args..., kwargs...)
        if isnothing(q)
            if isnothing(time_of_failure)
                time_of_failure = now()
            end
            if now() - time_of_failure > retry_time
                break
            end
            sleep(Dates.Second(2^nretries) + Dates.Millisecond(rand(0:1000)))
            nretries += 1
        else
            return q
        end
    end
    error(failure_message)
end

sqs_get_queue_with_retries(args...; kwargs...) = run_with_retries(
    SQS.get_queue_url,
    args...;
    failure_message = "Queue for communicating results is nonexistent",
    kwargs...
)

@specialize