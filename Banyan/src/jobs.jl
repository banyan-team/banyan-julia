# Process-local dictionary mapping from job IDs to instances of `Job`
global jobs = Dict()

# TODO: Allow for different threads to use different jobs by making this
# thread-local. For now, we only allow a single `Job` for each process
# and no sharing between threads; i.e., the burden is on the user to make
# sure they are synchronizing access to the `Job` if using the same one from
# different threads.
# TODO: Allow for different threads to use the same job by wrapping each
# `Job` in `jobs` in a mutex to allow only one to use it at a time. Further
# modifications would be required to make sharing a job between threads
# ergonomic.
global current_job_id = nothing

function set_job(job_id::Union{JobId,Nothing})
    global current_job_id
    current_job_id = job_id
end

function get_job_id()::JobId
    global current_job_id
    if isnothing(current_job_id)
        error(
            "No job selected using `create_job` or `with_job` or `set_job`. The current job may have been destroyed or no job created yet.",
        )
    end
    current_job_id
end

function get_job(job_id=get_job_id())::Job
    global jobs
    if !haskey(jobs, job_id)
        error("The selected job does not have any information; if it was created by this process, it has either failed or been destroyed.")
    end
    jobs[job_id]
end

get_cluster_name() = get_job().cluster_name
