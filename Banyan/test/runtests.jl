using Test
using Banyan

cluster_id = "pcluster-12-23"
set_cluster_id(cluster_id)
config = JobConfig(cluster_id, 2)
create_job(config, make_current = true)

@testset "Basic Tests" begin
    include("test_basic.jl")
end

#@testset "BLAS" begin
#    include("blas.jl")
#end

destroy_job()
