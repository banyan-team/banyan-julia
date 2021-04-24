# TODO: Support multi-threaded usage by storing a global array with a job ID
# for each thread
global current_job_id = nothing

function get_job_id()::JobId
    global current_job_id
    current_job_id
end

function create_job(;
    cluster_name::String = "",
    nworkers::Integer = 2,
    banyanfile_path::String = "",
    kwargs...,
)

    global current_job_id
    @debug "Creating job"

    if cluster_name == ""
        cluster_name = nothing
    end
    if banyanfile_path == ""
        banyanfile_path = nothing
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
    )
    if !isnothing(banyanfile_path)
        banyanfile = load_json(banyanfile_path)
        for included in banyanfile["include"]
            merge_banyanfile_with!(banyanfile, included, :code)
        end
        job_configuration["banyanfile"] = banyanfile
    end

    # Create the job
    @debug "Sending request for job creation"
    job_id = send_request_get_response(:create_job, job_configuration)

    # print(job_id)
    job_id = job_id["job_id"]
    # println("Creating job $job_id")

    # Store in global state
    current_job_id = job_id

    @debug "Finished creating job $job_id"
    return job_id
end

function destroy_job(job_id::JobId; kwargs...)
    global current_job_id

    # configure(; kwargs...)

    # @debug "Destroying job"
    #println("destroying job ", job_id, " now?")
    send_request_get_response(
        :destroy_job,
        Dict{String,Any}("job_id" => job_id),
    )

    if current_job_id == job_id
        current_job_id = nothing
    end
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

mutable struct Job
    job_id::JobId

    # function Job(; kwargs...)
    #     new_job_id = create_job(; kwargs...)
    #     #new_job_id = create_job(;cluster_name="banyancluster", nworkers=2)
    #     new_job = new(new_job_id)
    #     finalizer(new_job) do j
    #         destroy_job(j.job_id)
    #     end

    #     new_job
    # end
end

function Job(f::Function; kwargs...)
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
