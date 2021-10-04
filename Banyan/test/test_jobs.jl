using Pkg
using Statistics

IRIS_DOWNLOAD_PATH = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"


@testset "Create job using remote BanyanDataFrames Github with $pf_dispatch_table pf_dispatch_table" for pf_dispatch_table in 
    ["default", "http path"]

    Pkg.activate("./")

    if pf_dispatch_table == "default"
        pf_dispatch_table = ""
    elseif pf_dispatch_table == "http path"
        pf_dispatch_table = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib_info.json"
    end
    files = [IRIS_DOWNLOAD_PATH]

    # Create job
    job_id = create_job(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
        files = files,
        pf_dispatch_table = pf_dispatch_table,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = "remove_banyanfile",  # TODO: Change to "v0.1.3",
        directory = "banyan-julia/BanyanDataFrames/test",
        dev_paths = [
            "banyan-julia/Banyan",
            "banyan-julia/BanyanDataFrames",
        ],
        force_reclone = true
    )
    @test get_job_id() == job_id
    @test get_job().cluster_name == ENV["BANYAN_CLUSTER_NAME"]
    @test get_job().nworkers == 2
    @test get_cluster_name() == ENV["BANYAN_CLUSTER_NAME"]

    # Describe jobs
    curr_jobs = get_jobs(ENV["BANYAN_CLUSTER_NAME"], status="running")
    @test haskey(curr_jobs, job_id)
    @test curr_jobs[job_id]["status"] == "running"

    # Perform computation 1
    bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
    df = read_csv("s3://$(bucket_name)/iris.csv")
    gdf = groupby(df, :species)
    pl_means = combine(gdf, :petal_length => mean)
    res = collect(pl_means)
    @test res[:, :petal_length_mean] == [1.464, 4.26, 5.552]
    @test res[:, :species] == ["setosa", "versicolor", "virginica"]

    # Destroy job
    destroy_job(job_id)
    curr_jobs = get_jobs(ENV["BANYAN_CLUSTER_NAME"], status="running")
    @test !haskey(curr_jobs, job_id)
end

@testset "Create job using local Julia environment" begin
    # Activate an environment
    Pkg.activate("envs/")

    # Import some packages
    using Statistics
    using Distributions

    # Create a job
end

# @testset "Create job using remote Github environment"
    # Pkg.activate("./")
# end
