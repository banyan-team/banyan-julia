@testset "Create/Destroy jobs" begin
    config = JobConfig("pcluster-12-23", 2)
    create_job(config, true)
    destroy_job()
end
