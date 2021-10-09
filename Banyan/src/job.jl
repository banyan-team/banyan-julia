mutable struct Job
    id::JobId
    nworkers::Int32
    sample_rate::Int32
    locations::Dict{ValueId,Location}
    pending_requests::Vector{Request}
    # This is a `WeakKeyDict` so that futures can be GC-ed as long as all
    # references elsewhere are gone.
    futures_on_client::WeakKeyDict{ValueId,Future}
    cluster_name::String
    current_status::String

    # This struct just stores local state for the job.
    function Job(cluster_name::String, job_id::JobId, nworkers::Integer, sample_rate::Integer)::Job
        new(job_id, nworkers, sample_rate, Dict(), [], Dict(), cluster_name)
    end
end


# mutable struct Job
#     job_id::JobId
#     failed::Bool

#     # function Job(; kwargs...)
#     #     new_job_id = create_job(; kwargs...)
#     #     #new_job_id = create_job(;cluster_name="banyancluster", nworkers=2)
#     #     new_job = new(new_job_id)
#     #     finalizer(new_job) do j
#     #         destroy_job(j.job_id)
#     #     end

#     #     new_job
#     # end
# end
