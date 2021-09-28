using Statistics

IRIS_DOWNLOAD_PATH = "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"


@testset "Create job using remote Github" begin
    files = ["https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib.jl"]
    pt_lib_info = ""
    # for pt_lib_info in [
    #     "", "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib_info.json"
    # ],
    # for files in [
    #     [],
    #     ["https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/Banyan/res/pt_lib.jl"]
    # ]

    # Create job
    job_id = create_job(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
        files = vcat([IRIS_DOWNLOAD_PATH], files),
        pt_lib_info = pt_lib_info,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = "v0.1.3",
        directory = "banyan-julia/BanyanDataFrames",
    )
    @test get_job_id() == job_id
    @test get_job().cluster_name == ENV["BANYAN_CLUSTER_NAME"]
    @test get_job().nworkers == 2
    @test get_cluster_name() == ENV["BANYAN_CLUSTER_NAME"]

    # Describe jobs
    curr_jobs = get_jobs(ENV["BANYAN_CLUSTER_NAME"], status="running")
    @test length(curr_jobs) == 1
    @test haskey(curr_jobs, job_id)
    @test curr_jobs[job_id]["status"] == "running"

    # Perform computation 1
    bucket_name = get_cluster_s3_bucket_name()
    df = read_csv("s3://$(bucket_name)/iris.csv")
    gdf = groupby(df, :species)
    pl_means = combine(gdf, :petal_length => mean)
    res = collect(pl_means)
    @test res[:, :petal_length_mean] == [1.464, 4.26, 5.552]
    @test res[:, :species] == ["setosa", "versicolor", "virginica"]

    # Perform computation 2


    # Destroy job
    destroy_job(job_id)
    curr_jobs = get_jobs(ENV["BANYAN_CLUSTER_NAME"], status="running")
    @test !haskey(curr_jobs, job_id)
end

# @testset "Create job using local Julia environment"
# end

# #
# function test_jobs(test_job_failed)
#     if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#         error("Please provide BANYAN_CLUSTER_NAME for testing.")
#     end
#     # Create job
#     job_id = Banyan.create_job(
#         user_id = get(ENV, "BANYAN_USER_ID", nothing),
#         api_key = get(ENV, "BANYAN_API_KEY", nothing),
#         cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
#         nworkers = 2,
#         banyanfile_path = "file://res/Banyanfile.json",
#     )
#     @test get_job_id() == job_id
#     @test get_job().cluster_name == get(ENV, "BANYAN_CLUSTER_NAME", nothing)
#     @test get_job().nworkers == 2
#     @test get_cluster_name() == get(ENV, "BANYAN_CLUSTER_NAME", nothing)
#     # Describe jobs
#     curr_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")
#     @test length(curr_jobs) == 1
#     @test haskey(curr_jobs, job_id)
#     @test curr_jobs[job_id]["status"] == "running"
#     # Destroy job
#     destroy_job(job_id, failed = test_job_failed)
#     @test length(get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")) == 0
#     if test_job_failed
#         failed_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="failed")
#         @test haskey(failed_jobs, job_id)
#         @test failed_jobs[job_id]["status"] == "failed"
#     else
#         completed_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="completed")
#         @test haskey(completed_jobs, job_id)
#         @test completed_jobs[job_id]["status"] == "completed"
#     end
# end

# # 
# function test_concurrent_jobs()
#     if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#         error("Please provide BANYAN_CLUSTER_NAME for testing.")
#     end
#     # Create 2 jobs
#     job_id_1 = Banyan.create_job(
#         user_id = get(ENV, "BANYAN_USER_ID", nothing),
#         api_key = get(ENV, "BANYAN_API_KEY", nothing),
#         cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
#         nworkers = 2,
#         banyanfile_path = "file://res/Banyanfile.json",
#     )
#     job_id_2 = Banyan.create_job(
#         user_id = get(ENV, "BANYAN_USER_ID", nothing),
#         api_key = get(ENV, "BANYAN_API_KEY", nothing),
#         cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
#         nworkers = 2,
#         banyanfile_path = "file://res/Banyanfile.json",
#     )
#     curr_jobs = get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="running")
#     @test length(curr_jobs) == 2
#     @test haskey(curr_jobs, job_id_1)
#     @test haskey(curr_jobs, job_id_2)
#     # Destroy all jobs
#     destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#     completed_jobs =
#         get_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing), status="completed")
#     @test haskey(completed_jobs, job_id_1)
#     @test haskey(completed_jobs, job_id_2)
#     @test completed_jobs[job_id_1]["status"] == "completed"
#     @test completed_jobs[job_id_2]["status"] == "completed"
# end


# @testset "Test simple job management" begin
#     if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#         error("Please provide BANYAN_CLUSTER_NAME for testing.")
#     end
#     run("create/destroy job") do
#         try
#             destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#             test_jobs(false)
#             test_jobs(true)
#         catch
#             destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#             rethrow()
#         end
#     end
# end

# @testset "Test concurrent jobs" begin
#     if isnothing(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#         error("Please provide BANYAN_CLUSTER_NAME for testing.")
#     end
#     run("create/destroy concurrent jobs") do
#         try
#             destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#             test_concurrent_jobs()
#         catch
#             destroy_all_jobs(get(ENV, "BANYAN_CLUSTER_NAME", nothing))
#             rethrow()
#         end
#     end
# end
