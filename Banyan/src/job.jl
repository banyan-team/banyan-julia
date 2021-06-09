struct Job
    id::JobId
    nworkers::Int32
    sample_rate::Int32
    locations::Dict{ValueId,Location}
    pending_requests::Vector{Request}
    futures_on_client::WeakKeyDict{ValueId,Future}

    # TODO: Ensure that this struct and constructor (which are just for storing
    # information about the job) does not conflict with the `Job` function that
    # calls `create_job`
    function Job(job_id::JobId, nworkers::Integer, sample_rate::Integer)::Job
        new(job_id, nworkers, sample_rate, Dict(), [], Dict())
    end
end
