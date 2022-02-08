# Tests for Sessions:
#  Start a session that creates a new job
#  Start a session that reuses a job
#    Previous session was successfully ended (by calling end_session with delayed destruction)
#    Previous session had a session failure 

@testset "Get sessions with status $status" for status in [
    "all",
    "creating",
    "running",
    "failed",
    "completed",
    "invalid_status"
]
    # Start a session 
    Pkg.activate("./")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    if status == "all"
        sessions = get_sessions(cluster_name)
    else
        filtered_sessions = get_sessions(cluster_name, status=status)
        @test all(j -> j[2]["status"] == status, filtered_sessions)
    end
end

@testset "Get running sessions" begin
    # Start a session 
    Pkg.activate("./")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]    
    
    session_id = start_session()
    running_sessions = get_running_sessions(cluster_name)
    end_session(session_id, release_resources_now=true)
    sessions = get_sessions(cluster_name)

    @test all(j -> j[2]["status"] == status, running_sessions)
    @test any(j -> j[1] == session_id, running_sessions)
    @test any(j -> (j[1] == session_id && j[2]["status"] == "completed"), sessions)
end


@testset "Start a session" begin
    start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
        print_logs = true,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
        directory = "banyan-julia/Banyan",
        force_pull = get(ENV, "BANYAN_FORCE_CLONE", "0") == "0",
        force_clone = get(ENV, "BANYAN_FORCE_CLONE", "0") == "1",
        force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
        store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
    )
    println("JOB ID: ", get_session().resource_id)
end



# Test that starting a second session after one has been ended
# reuses the same job, if the parameters match.
@testset "Start and end multiple sessions" begin
    Pkg.activate("envs/DataAnalysisProject/")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    delay_time = 5

    # Start a session and end it
    session_id_1 = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
        print_logs = true,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
        directory = "banyan-julia/Banyan/test",
        dev_paths = ["banyan-julia/Banyan"],
        store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
        release_resources_after=delay_time
    )
    println("SESSION_ID: ", session_id_1)
    println("JOB ID :", get_session().resource_id)
    # resource_id_1 = get_session().resource_id
    # session_status = get_session_status(session_id_1)
    # @test session_status == "running"

    end_session(session_id_1)
    # sleep(60)
    # session_status = get_session_status(session_id_1)
    # @test session_status == "completed"

    # # Start another session with same nworkers and verify the job ID matches
    # session_id_2 = start_session(
    #     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
    #     nworkers = 2,
    #     print_logs = true,
    #     url = "https://github.com/banyan-team/banyan-julia.git",
    #     branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
    #     directory = "banyan-julia/Banyan/test",
    #     dev_paths = ["banyan-julia/Banyan"],
    #     store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
    #     release_resources_after=delay_time
    # )
    # resource_id_2 = get_session().resource_id
    # session_status = get_session_status(session_id_2)
    # @test session_status == "running"
    # @test resource_id_2 == resource_id_1
    
    # end_session(session_id_2)
    # sleep(60)
    # session_status = get_session_status(session_id_2)
    # @test session_status == "completed"

    # # Start another session with different nworkers and verify the job ID
    # # is different
    # session_id_3 = start_session(
    #     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
    #     nworkers = 2,
    #     print_logs = true,
    #     url = "https://github.com/banyan-team/banyan-julia.git",
    #     branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
    #     directory = "banyan-julia/Banyan/test",
    #     dev_paths = ["banyan-julia/Banyan"],
    #     store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
    #     release_resources_after=delay_time
    # )
    # resource_id_3 = get_session().resource_id
    # session_status = get_session_status(session_id_3)
    # @test session_status == "running"
    # @test resource_id_3 != resource_id_1
    
    # end_session(session_id_3)
    # sleep(60)
    # session_status = get_session_status(session_id_3)
    # @test session_status == "completed"

    # # Sleep for the delay_time and check that the sessions are completed
    # # by creating a new session
    # sleep(delay_time * 60)
    # session_id_4 = start_session(
    #     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
    #     nworkers = 2,
    #     print_logs = true,
    #     url = "https://github.com/banyan-team/banyan-julia.git",
    #     branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
    #     directory = "banyan-julia/Banyan/test",
    #     dev_paths = ["banyan-julia/Banyan"],
    #     store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
    #     release_resources_after=delay_time,
    #     nowait=true
    # )
    # resource_id_4 = get_session().resource_id
    # @test resource_id_4 != resource_id_1
    
    # end_session(session_id_4)
end

@testset "Create sessions with nowait=$nowait" for
        nowait in [true, false]
    Pkg.activate("envs/DataAnalysisProject/")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    session_id = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
        print_logs = true,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
        directory = "banyan-julia/Banyan",
        force_pull = get(ENV, "BANYAN_FORCE_CLONE", "0") == "0",
        force_clone = get(ENV, "BANYAN_FORCE_CLONE", "0") == "1",
        force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
        store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
        nowait=nowait
    )

    session_status = get_session_status(session_id)
    if !nowait
        @test session_status == "running"
    else
        @test session_status == "creating"
        while session_status == "creating"
            sleep(20)
            session_status = get_session_status(session_id)
        end
        @test session_status == "running"
    end

    end_session(session_id, force=true)
end

@testset "Create sessions where store_logs_in_s3=$store_logs_in_s3" for 
        store_logs_in_s3 in [true, false]
    Pkg.activate("./")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    job_id = start_session(
        cluster_name=cluster_name,
        store_logs_in_s3=store_logs_in_s3,
    )
    end_session(job_id)
    sleep(10)

    log_file = "banyan-log-for-job-$job_id"
    println("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)")
    @test store_logs_in_s3 == isfile(
        S3Path("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)",
        config=Banyan.get_aws_config())
    )
end

@testset "Create sessions using $env_type environment" for env_type in ["local", "remote"]
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    if env_type == "remote"
        Pkg.activate("./")

        # Create job
        job_id = start_session(
            cluster_name = cluster_name,
            nworkers = 2,
            pf_dispatch_table = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pf_dispatch_table.json",
            url = "https://github.com/banyan-team/banyan-julia.git",
            branch = "v0.1.3",
            directory = "banyan-julia/BanyanDataFrames/test",
            dev_paths = [
                "banyan-julia/Banyan",
                "banyan-julia/BanyanDataFrames",
            ],
            force_clone = true,
            force_pull = true,
            force_install = true,
        )

        session_status = get_session_status(job_id)
        @test session_status == "running"

        # Destroy job
        end_session(job_id)

    elseif env_type == "envs"
        # Activate environment
        Pkg.activate("envs/DataAnalysisProject/")

        # Import packages
        using Distributions
        using Statistics

        # Test environment detection
        env_dir = get_julia_environment_dir()
        loaded_packages = get_loaded_packages()

        @test abspath(env_dir) == abspath("envs/DataAnalysisProject/")
        @test "Distributions" in loaded_packages && "Statistics" in loaded_packages

        # Create job
        job_id = start_session(
            cluster_name=cluster_name,
            print_logs=false,
            store_logs_in_s3=false,
            store_logs_on_cluster=false,
            session_name="testsession2",
            files=["data/iris.csv"],
            code_files=["envs/DataAnalysisProject/analysis.jl"],
            force_update_files=true,
        )

        # Destroy job
        end_session(job_id)
    end

end
