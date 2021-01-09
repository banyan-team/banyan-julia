#@testset "Create/Destroy jobs" begin
#    cluster_id = "pcluster-12-23"
#    set_cluster_id(cluster_id)
#    config = JobConfig(cluster_id, 2)
#    create_job(config, make_current = true)
#    destroy_job()
#end

<<<<<<< HEAD
@testset "Simple annotation" begin
    x = Future([1, 2, 3, 4, 5])
    # @pa x, Dict(x => "Mut"), PartitionAnnotation(Partitions(Dict()), PartitioningConstraints(Set())) begin
    #     z = 10
    #     println("hello ", z)
    # end
    @pa x, Dict(x => "Mut"), PartitionAnnotation(Partitions(Dict(x.value_id => PartitionType("Block", "Block", [], [], -1))), PartitioningConstraints(Set())) begin
        z = 10
        println("hello ", z)
    end
    evaluate(x)
end
=======
#@testset "Simple annotation" begin
#    x = Future(49)
#    # @pa x, Dict(x => "Mut"), PartitionAnnotation(Partitions(Dict(x.value_id => [Value(x)])), PartitioningConstraints(Set())) begin
#    @pa x, Dict(x => "Mut"), pa_noconstraints(Dict(x.value_id => [Value(x)])) begin
#        z = 10
#        println("hello ", z)
#    end
#    evaluate(x)
#end
>>>>>>> 06fad0591e5414d7b8ccd90b9d6d11d6189ad897

# @testset "Location Type" begin
#     x = Future([1, 2, 3, 4, 5])
#     @lt x LocationType("New", "HDF5", [], [], 1024)
#     println("done")
# end

@testset "Simple annotation with Block" begin
    y = Future()

    @pa y, Dict(y => "Mut"), pa_noconstraints(Dict(y.value_id => [Block(1)])) begin
        y = fill(1, 16)
    end

    @pa y, Dict(y => "Mut"), pa_noconstraints(Dict(y.value_id => [Block(1)])) begin
        y = y * 2
        println("hello ", y)
    end

    evaluate(y)
end
