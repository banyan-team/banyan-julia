

#
function test_jobs(test_job_failed)
    if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
        error("Please provide BANYAN_CLUSTER_NAME for testing.")
    end
    # Create job
    job_id = Banyan.create_job(
        user_id = get(ENV, "BANYAN_USER_ID", nothing),
        api_key = get(ENV, "BANYAN_API_KEY", nothing),
        cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
        nworkers = 2,
        banyanfile_path = "file://res/Banyanfile.json",
    )
    @test get_job_id() == job_id
    @test get_job().cluster_name == get(ENV, "BANYAN_CLUSTER_NAME", nothing)
    @test get_job().nworkers == 2
    @test get_cluster_name() == get(ENV, "BANYAN_CLUSTER_NAME", nothing)
    # Describe jobs
    curr_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")
    @test length(curr_jobs) == 1
    @test haskey(curr_jobs, job_id)
    @test curr_jobs[job_id]["status"] == "running"
    # Destroy job
    destroy_job(job_id, failed = test_job_failed)
    @test length(get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")) == 0
    if test_job_failed
        failed_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="failed")
        @test haskey(failed_jobs, job_id)
        @test failed_jobs[job_id]["status"] == "failed"
    else
        completed_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="completed")
        @test haskey(completed_jobs, job_id)
        @test completed_jobs[job_id]["status"] == "completed"
    end
end

# 
function test_concurrent_jobs()
    if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
        error("Please provide BANYAN_CLUSTER_NAME for testing.")
    end
    # Create 2 jobs
    job_id_1 = Banyan.create_job(
        user_id = get(ENV, "BANYAN_USER_ID", nothing),
        api_key = get(ENV, "BANYAN_API_KEY", nothing),
        cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
        nworkers = 2,
        banyanfile_path = "file://res/Banyanfile.json",
    )
    job_id_2 = Banyan.create_job(
        user_id = get(ENV, "BANYAN_USER_ID", nothing),
        api_key = get(ENV, "BANYAN_API_KEY", nothing),
        cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
        nworkers = 2,
        banyanfile_path = "file://res/Banyanfile.json",
    )
    curr_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")
    @test length(curr_jobs) == 2
    @test haskey(curr_jobs, job_id_1)
    @test haskey(curr_jobs, job_id_2)
    # Destroy all jobs
    destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
    completed_jobs =
        get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="completed")
    @test haskey(completed_jobs, job_id_1)
    @test haskey(completed_jobs, job_id_2)
    @test completed_jobs[job_id_1]["status"] == "completed"
    @test completed_jobs[job_id_2]["status"] == "completed"
end


@testset "Test simple job management" begin
    if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
        error("Please provide BANYAN_CLUSTER_NAME for testing.")
    end
    run("create/destroy job") do
        try
            destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
            test_jobs(false)
            test_jobs(true)
        catch
            destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
            rethrow()
        end
    end
end

@testset "Test concurrent jobs" begin
    if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
        error("Please provide BANYAN_CLUSTER_NAME for testing.")
    end
    run("create/destroy concurrent jobs") do
        try
            destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
            test_concurrent_jobs()
        catch
            destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
            rethrow()
        end
    end
end
