@testset "Create/Destroy jobs" begin
    cluster_id = "pcluster-12-23"
    set_cluster_id(cluster_id)
    config = JobConfig(cluster_id, 2)
    create_job(config, make_current = true)
    destroy_job()
end
