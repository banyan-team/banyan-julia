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

@testset "Create jobs where store_logs_in_s3=$store_logs_in_s3" for 
        store_logs_in_s3 in [true, false]
    Pkg.activate("./")
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    job_id = create_job(
        cluster_name=cluster_name,
        store_logs_in_s3=store_logs_in_s3
    )
    destroy_job(job_id)
    sleep(10)

    log_file = "banyan-log-for-job-$job_id"
    println("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)")
    @test store_logs_in_s3 == isfile(
        S3Path("s3://$(get_cluster_s3_bucket_name(cluster_name))/$(log_file)",
        config=Banyan.get_aws_config())
    )
end

@testset "Create jobs using $env_type environment" for env_type in ["local", "remote"]
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    if env_type == "remote"
        Pkg.activate("./")

        # Create job
        job_id = create_job(
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

        # Destroy job
        destroy_job(job_id)

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
        job_id = create_job(
            cluster_name=cluster_name,
            print_logs=false,
            store_logs_in_s3=false,
            store_logs_on_cluster=false,
            job_name="testjob2",
            files=["data/iris.csv"],
            code_files=["envs/DataAnalysisProject/analysis.jl"],
            force_update_files=true,
        )

        # Destroy job
        destroy_job(job_id)
    end

end
