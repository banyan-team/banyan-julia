using Test
using Banyan

# TODO: Uncomment
#cluster_id = "banyantest"
#set_cluster_id(cluster_id)
#config = JobRequest(cluster_id, 2)
#create_job(config, make_current = true)

@testset "Basic Tests" begin
    include("test_basic.jl")
end

#@testset "BLAS" begin
#    include("blas.jl")
#end

#destroy_job()
