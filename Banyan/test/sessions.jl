# Test that starting a second session after one has been ended
# reuses the same job, if the parameters match.
@testset "Start and end multiple sessions"
    Pkg.activate("envs/DataAnalysisProject/")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    # Start a session and end it
    job_id_1 = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        resource_destruction_delay=3
    )
    session_status = get_session_status(job_id_1)
    @test session_status == "running"

    end_session(job_id_1)
    session_status = get_session_status(job_id_1)
    @test session_status == "completed"

    # Start another session with same nworkers and verify the job ID matches
    job_id_2 = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        resource_destruction_delay=3
    )
    session_status = get_session_status(job_id_2)
    @test session_status == "running"
    @test job_id_2 == job_id_1
    
    end_session(job_id_2)
    session_status = get_session_status(job_id_2)
    @test session_status == "completed"

    # Start another session with different nworkers and verify the job ID
    # is different
    job_id_3 = start_session(
        cluster_name=cluster_name,
        nworkers=3,
        resource_destruction_delay=5
    )
    @test session_status == "running"
    @test job_id_3 != job_id_1
    
    end_session(job_id_3)
    session_status = get_session_status(job_id_3)
    @test session_status == "completed"
end