using Banyan, BanyanImages

using ReTest
using AWSS3, FileIO, ImageIO, ImageCore, Random, MPI
using Downloads, JSON

MPI.Init()

# Create a dummy test job for unit tests
test_job_id = "test_job_id"
Banyan.jobs[test_job_id] = Job(ENV["BANYAN_CLUSTER_NAME"], test_job_id, 2, 2)

include("utils_data.jl")
include("locations.jl")
include("png.jl")
include("jpg.jl")
include("pfs.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up.
    # destroy_all_jobs_for_testing()
    cleanup_s3_test_files(get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"]))
end