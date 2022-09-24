# # Tests for Sessions:
# #  Start a session that creates a new job
# #  Start a session that reuses a job
# #    Previous session was successfully ended (by calling end_session with delayed destruction)
# #    Previous session had a session failure 

# @testset "Get sessions with status $status" for status in [
#     "all",
#     "creating",
#     "running",
#     "failed",
#     "completed",
#     "invalid_status"
# ]
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     if status == "all"
#         sessions = get_sessions(cluster_name)
#     else
#         filtered_sessions = get_sessions(cluster_name, status=status)
#         @test all(s -> s[2]["status"] == status, filtered_sessions)
#     end
# end

# @testset "Get running sessions" begin
#     # Start a session 
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]    
    
#     session_id = start_session(cluster_name=cluster_name, nworkers=2)
#     running_sessions = get_running_sessions(cluster_name)
#     end_session(session_id, release_resources_now=true)
#     sessions = get_sessions(cluster_name)

#     @test all(s -> s[2]["status"] == "running", running_sessions)
#     @test any(s -> s[1] == session_id, running_sessions)
#     @test any(s -> (s[1] == session_id && s[2]["status"] == "completed"), sessions)
# end

# # Test that starting a second session after one has been ended
# # reuses the same job, if the parameters match.
# @testset "Start and end multiple sessions" begin
#     # Pkg.activate("envs/DataAnalysisProject/")
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]
#     delay_time = 5

#     # Start a session and end it
#     session_id_1 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         force_synce = true,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         release_resources_after=delay_time
#     )
#     resource_id_1 = get_session().resource_id
#     session_status = get_session_status(session_id_1)
#     @test session_status == "running"

#     end_session(session_id_1)
#     sleep(60) # To ensure session gets ended
#     session_status = get_session_status(session_id_1)
#     @test session_status == "completed"

#     # Start another session with same nworkers and verify the job ID matches
#     session_id_2 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         release_resources_after=delay_time
#     )
#     resource_id_2 = get_session().resource_id
#     session_status = get_session_status(session_id_2)
#     @test session_status == "running"
#     @test resource_id_2 == resource_id_1  # it should have reused resource
    
#     end_session(session_id_2)
#     sleep(60)
#     session_status = get_session_status(session_id_2)
#     @test session_status == "completed"

#     # Start another session with different nworkers and verify the job ID
#     # is different
#     session_id_3 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 4,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         release_resources_after=delay_time
#     )
#     resource_id_3 = get_session().resource_id
#     session_status = get_session_status(session_id_3)
#     @test session_status == "running"
#     @test resource_id_3 != resource_id_1
    
#     end_session(session_id_3)
#     sleep(60)
#     session_status = get_session_status(session_id_3)
#     @test session_status == "completed"

#     # Sleep for the delay_time and check that the underlying resources are destroyed
#     # by creating a new session and ensuring that it uses different resources
#     sleep(delay_time * 60)
#     session_id_4 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         release_resources_after=delay_time,
#         wait_now=false
#     )
#     resource_id_4 = get_session().resource_id
#     @test resource_id_4 != resource_id_1
    
#     end_session(session_id_4, release_resources_now=true)
# end

# @testset "Start a session with dev paths" begin
#     session_id = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         url = "https://github.com/banyan-team/banyan-julia.git",
#         branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
#         directory = "banyan-julia/Banyan/test",
#         dev_paths = [
#             "banyan-julia/Banyan",
#         ],
#         force_pull = true,
#         force_sync = true,
#         force_install = true,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#     )
#     session_status = get_session_status(session_id)
#     end_session(session_id, release_resources_now=true)
#     @test session_status == "running"
# end

# @testset "Create sessions with nowait=$nowait" for
#         nowait in [true, false]
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     session_id = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         nowait=nowait
#     )

#     session_status = get_session_status(session_id)
#     if !nowait
#         @test session_status == "running"
#     else
#         @test session_status == "creating"
#         while session_status == "creating"
#             sleep(20)
#             session_status = get_session_status(session_id)
#         end
#         @test session_status == "running"
#     end

#     end_session(session_id, release_resources_now=true)
# end

# @testset "Create sessions where store_logs_in_s3=$store_logs_in_s3" for 
#         store_logs_in_s3 in [true, false]
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     session_id = start_session(
#         cluster_name=cluster_name,
#         nworkers = 2,
#         store_logs_in_s3=store_logs_in_s3,
#     )
#     end_session(session_id, release_resources_now=true)
#     sleep(60)

#     log_file = "banyan-log-for-session-$session_id"
#     println("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)")
#     @test store_logs_in_s3 == isfile(
#         S3Path("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)",
#         config=Banyan.global_aws_config())
#     )
# end

# @testset "Starting session with failure in $scenario" for scenario in [
#     "invalid julia version",
#     "invalid branch name",
#     "invalid dev paths"
# ]
#     Pkg.activate("./")

#     try
#         if scenario == "invalid julia version"
#             # Temporarily overwrite `get_julia_version`
#             Banyan.get_julia_version() = "invalidversion"
#             @test_throws begin
#                 session_id = start_session(
#                     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#                     nworkers = 2,
#                     store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#                 )
#             end ErrorException
#         elseif scenario == "invalid branch name"
#             @test_throws begin
#                 session_id = start_session(
#                     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#                     nworkers = 2,
#                     url = "https://github.com/banyan-team/banyan-julia.git",
#                     branch = "nonexistant-branch",
#                     directory = "banyan-julia/Banyan/test",
#                     dev_paths = [
#                         "banyan-julia/Banyan",
#                     ],
#                     force_pull = true,
#                     force_sync = true,
#                     force_install = true,
#                 )
#             end ErrorException
#         elseif scenario == "invalid dev paths"
#             @test_throws begin
#                 session_id = start_session(
#                     cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#                     nworkers = 2,
#                     url = "https://github.com/banyan-team/banyan-julia.git",
#                     branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
#                     directory = "banyan-julia/Banyan/test",
#                     dev_paths = [
#                         "banyan-julia/Banyan",
#                         "banyan-julia/NonExistantPackage"
#                     ],
#                     force_pull = true,
#                     force_sync = true,
#                     force_install = true,
#                 )
#             end ErrorException
#         end
#     catch
#     end
# end

# @testset "Reusing session that fails" begin
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     # Start a session
#     session_id_1 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         force_sync=true
#     )
#     resource_id_1 = get_session().resource_id
#     session_status_1 = get_session_status(session_id_1)

#     # Trigger a failure in the session that will end the session
#     try
#         @test_throws begin
#             offloaded(distributed=true) do
#                 error("Oops sorry this is an error")
#             end
#         end ErrorException
#     catch
#     end
#     session_status_1_after_failure = get_session_status(session_id_1)

#     # Start a new session (it should reuse the resources of the failed session) and then end it
#     session_id_2 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         wait_now=false
#     )
#     resource_id_2 = get_session().resource_id
#     session_status_2 = get_session_status(session_id_2)
#     end_session(session_id_2, release_resources_now=true)

#     # Assert
#     @test session_status_1 == "running"
#     @test session_status_1_after_failure == "failed"
#     @test resource_id_2 == resource_id_1
# end

@testset "Running session with print_logs=$print_logs and store_logs_in_s3=$store_logs_in_s3 and log_initialization=$log_initialization" for
    print_logs in [true, false],
    (store_logs_in_s3, log_initialization) in [(false, false), (true, false), (true, true)]

    println("Before run_session")
    run_session(
        "file://run_session_test_script.jl",
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers=1,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory = "banyan-julia/Banyan/test",
        dev_paths = [
            "banyan-julia/Banyan",
        ],
        print_logs = print_logs,
        store_logs_in_s3 = store_logs_in_s3,
        store_logs_on_cluster=true,
        instance_type="t3.large",
        disk_capacity="auto",
        log_initialization=log_initialization,
        force_pull=true
    )
end

@testset "Starting session with print_logs=$print_logs" for print_logs in [true, false]
    s = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers=1,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory = "banyan-julia/Banyan/test",
        dev_paths = [
            "banyan-julia/Banyan",
        ],
        instance_type="t3.large",
        disk_capacity="auto",
        print_logs=print_logs,
        store_logs_on_cluster=true,
        force_pull=true
    )
    @test get_session().id == get_session_id()
    offloaded() do
        @debug "This is a test debug statement"
        @info "This is a test info statement"
        @info "This is a test println statement"
    end
    offloaded(print_logs=true) do
        @debug "This is a test debug statement with print_logs=true"
        @info "This is a test info statement with print_logs=true"
        @info "This is a test println statement with print_logs=true"
    end
    offloaded(print_logs=false) do
        @debug "This is a test debug statement with print_logs=false"
        @info "This is a test info statement with print_logs=false"
        @info "This is a test println statement with print_logs=false"
    end
    end_session(s)

    s = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers=1,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory = "banyan-julia/Banyan/test",
        dev_paths = [
            "banyan-julia/Banyan",
        ],
        start_now=true,
        instance_type="t3.large",
        disk_capacity="auto",
        print_logs=print_logs
    )
    @test get_session().id == get_session_id()
    offloaded() do
        @debug "This is a test debug statement"
        @info "This is a test info statement"
        @info "This is a test println statement"
    end
    offloaded(print_logs=true) do
        @debug "This is a test debug statement with print_logs=true"
        @info "This is a test info statement with print_logs=true"
        @info "This is a test println statement with print_logs=true"
    end
    offloaded(print_logs=false) do
        @debug "This is a test debug statement with print_logs=false"
        @info "This is a test info statement with print_logs=false"
        @info "This is a test println statement with print_logs=false"
    end
    end_session(s, print_logs=true)

    s = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers=1,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory = "banyan-julia/Banyan/test",
        dev_paths = [
            "banyan-julia/Banyan",
        ],
        wait_now=true,
        instance_type="t3.large",
        disk_capacity="auto",
        print_logs=print_logs 
    )
    @test get_session().id == get_session_id()
    end_session(s)
end

# Outdated testset...revisit later...probably alread tested through above tests
# @testset "Create sessions using $env_type environment" for env_type in ["local", "remote"]
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     if env_type == "remote"
#         Pkg.activate("./")

#         # Create job
#         job_id = start_session(
#             cluster_name = cluster_name,
#             nworkers = 2,
#             pf_dispatch_table = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pf_dispatch_table.json",
#             url = "https://github.com/banyan-team/banyan-julia.git",
#             branch = "v0.1.3",
#             directory = "banyan-julia/BanyanDataFrames/test",
#             dev_paths = [
#                 "banyan-julia/Banyan",
#                 "banyan-julia/BanyanDataFrames",
#             ],
#             force_sync = true,
#             force_pull = true,
#             force_install = true,
#         )

#         session_status = get_session_status(job_id)
#         @test session_status == "running"

#         # Destroy job
#         end_session(job_id)

#     elseif env_type == "envs"
#         # Activate environment
#         Pkg.activate("envs/DataAnalysisProject/")

#         # Import packages
#         using Distributions
#         using Statistics

#         # Test environment detection
#         env_dir = get_julia_environment_dir()
#         loaded_packages = get_loaded_packages()

#         @test abspath(env_dir) == abspath("envs/DataAnalysisProject/")
#         @test "Distributions" in loaded_packages && "Statistics" in loaded_packages

#         # Create job
#         job_id = start_session(
#             cluster_name=cluster_name,
#             print_logs=false,
#             store_logs_in_s3=false,
#             store_logs_on_cluster=false,
#             session_name="testsession2",
#             files=["data/iris.csv"],
#             code_files=["envs/DataAnalysisProject/analysis.jl"],
#             force_update_files=true,
#         )

#         # Destroy job
#         end_session(job_id)
#     end
# end
