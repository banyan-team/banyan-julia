@testset "Create/Destroy jobs" begin
    cluster_id = "pcluster-12-23"
    set_cluster_id(cluster_id)
    config = JobConfig(cluster_id, 2)
    create_job(config, make_current = true)
    destroy_job()
end

@testset "Simple annotation" begin
	x = Future([1, 2, 3, 4, 5])
    @pa x PartitionAnnotation(Dict(), PartitioningConstraints([])) begin
        z = 10
        println("hello ", z)
    end
    # evaluate(x)
end