#@testset "Create/Destroy jobs" begin
#    cluster_id = "pcluster-12-23"
#    set_cluster_id(cluster_id)
#    config = JobConfig(cluster_id, 2)
#    create_job(config, make_current = true)
#    destroy_job()
#end

@testset "Simple annotation" begin
    x = Future([1, 2, 3, 4, 5])
    @pa x, Dict(x => "Mut"), PartitionAnnotation(Dict(), PartitioningConstraints(Set())) begin
        z = 10
        println("hello ", z)
    end
    evaluate(x)
end

# @testset "Location Type" begin
#     x = Future([1, 2, 3, 4, 5])
#     @lt x LocationType("New", "HDF5", [], [], 1024)
#     println("done")
# end
