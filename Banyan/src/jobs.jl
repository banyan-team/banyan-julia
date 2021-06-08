########
# Jobs #
########

struct Job
    id::JobId
    nworkers::Int32
    sample_rate::Int32
    locations::Dict{ValueId, Location}
    pending_requests::Vector{Request}
    futures_on_client::WeakKeyDict{ValueId, Future}

    # TODO: Ensure that this struct and constructor (which are just for storing
    # information about the job) does not conflict with the `Job` function that
    # calls `create_job`
    Job(job_id::JobId, nworkers::Integer, sample_rate::Integer)::Job =
        new(
            job_id,
            nworkers,
            sample_rate,
            Dict(),
            [],
            Dict()
        )
end

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
global current_job_status = nothing

function set_job_id(job_id::Union{JobId, Nothing})
    global current_job_id
    current_job_id = job_id
end

function get_job_id()::JobId
    global current_job_id
    if isnothing(current_job_id)
        error("No job selected using `set_job_id` or `create_job`")
    end
    current_job_id
end

function get_job()::Job
    global jobs
    jobs[get_job_id()]
end

function create_job(;
    cluster_name::String = nothing,
    nworkers::Integer = 2,
    banyanfile_path::String = "",
    logs_location::String = "",
    sample_rate::Integer = nworkers,
    kwargs...,
)

    global current_job_id
    global current_job_status
    @debug "Creating job"
    if cluster_name == ""
        cluster_name = nothing
    end
    if banyanfile_path == ""
        banyanfile_path = nothing
    end
    if logs_location == ""
        logs_location = "client"
    end

    # Configure
    configure(; kwargs...)
    cluster_name = if isnothing(cluster_name)
        clusters = list_clusters()
        if length(clusters) == 0
            error("Failed to create job: you don't have any clusters created")
        end
        first(keys(clusters))
    else
        cluster_name
    end

    # Merge Banyanfile if provided
    job_configuration = Dict{String,Any}(
        "cluster_name" => cluster_name,
        "num_workers" => nworkers,
	"logs_location" => "s3",  #logs_location,
    )
    if !isnothing(banyanfile_path)
        banyanfile = load_json(banyanfile_path)
        for included in banyanfile["include"]
            merge_banyanfile_with!(banyanfile, included, :job, :creation)
        end
        job_configuration["banyanfile"] = banyanfile
    end

    # Create the job
    @debug "Sending request for job creation"
    job_id = send_request_get_response(:create_job, job_configuration)
    job_id = job_id["job_id"]
    @debug "Creating job $job_id"

    # Store in global state
    current_job_id = job_id
    current_job_status = "running"

    @debug "Finished creating job $job_id"
    return job_id
end

function destroy_job(job_id::JobId; failed = false, kwargs...)
    global current_job_id
    global current_job_status

    failed = false
    if current_job_status == "failed"
    	failed = true
    end


    # configure(; kwargs...)

    @debug "Destroying job $job_id"
    send_request_get_response(
        :destroy_job,
        Dict{String,Any}("job_id" => job_id, "failed" => failed),
    )

    # Remove from global state
    if get_job_id() == job_id
        set_job_id(nothing)
    end
    delete!(jobs, job_id)
end

function get_jobs(; kwargs...)
    @debug "Downloading description of jobs in each cluster"
    configure(; kwargs...)
    response =
        send_request_get_response(:describe_jobs, Dict{String,Any}())
    response["jobs"]
end

function destroy_all_jobs(cluster_name::String; kwargs...)
    @debug "Destroying all jobs for cluster"
    configure(; kwargs...)
    jobs = get_jobs()[cluster_name]
    for (job_id, job) in jobs
        destroy_job(job_id; kwargs...)
    end
end

# destroy_job() = destroy_job(get_job_id())

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

function Job(f::Function; kwargs...)
    # This is not a constructor; this is just a function that ensures that
    # every job is always destroyed even in the case of an error
    j = create_job(;kwargs...)
    try
        f(j)
    finally
    	destroy_job(j)
    end
end

function clear_jobs()
    global pending_requests
    empty!(pending_requests)
end

# TODO: Fix bug causing nbatches to be 2 when it should be 25
# TODO: Fix finalizer of Job
