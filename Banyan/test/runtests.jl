using Test
using Banyan

# TODO: Uncomment
#cluster_id = "banyantest"
#set_cluster_id(cluster_id)
#config = JobRequest(cluster_id, 2)
#create_job(config, make_current = true)

clear_jobs()

include("test_l1_l2.jl")
include("test_l3.jl")

#@testset "BLAS" begin
#    include("blas.jl")
#end

#destroy_job()
