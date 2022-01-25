# Add an @testset that creates a job 
# (by calling create_job) and 
# then offloads a function (by calling offloaded) 
# that returns -1, and then 
# test that the call to offloaded returns the -1 

@testset "Offload Function" begin
    Pkg.activate("./")
    # cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    use_session_for_testing() do
        res = offloaded() do
            return -1
        end

        res2 = offloaded(()->-1)

        @test res == -1
        @test res2 == -1
    end
end





@testset "Get jobs" begin
    Pkg.activate("./")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    job_id = create_job()
    running_jobs = get_running_jobs(cluster_name)
    destroy_job(job_id)
    jobs = get_jobs(cluster_name)

    @test all(j -> j[2]["status"] == "running", running_jobs)
    @test any(j -> j[1] == job_id, running_jobs)
    @test any(j -> (j[1] == job_id && j[2]["status"] == "completed"), jobs)
end