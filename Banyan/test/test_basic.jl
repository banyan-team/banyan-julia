#@testset "Create/Destroy jobs" begin
#    cluster_id = "pcluster-12-23"
#    set_cluster_id(cluster_id)
#    config = JobRequest(cluster_id, 2)
#    create_job(config, make_current = true)
#    destroy_job()
#end

#@testset "Simple annotation" begin
#    x = Future(49)
#    # @pa x, Dict(x => "Mut"), PartitionAnnotation(Partitions(Dict(x.value_id => [Value(x)])), PartitioningConstraints(Set())) begin
#    @pa x, Dict(x => "Mut"), pa_noconstraints(Dict(x.value_id => [Value(x)])) begin
#        z = 10
#        println("hello ", z)
#    end
#    evaluate(x)
#end

# @testset "Location Type" begin
#     x = Future([1, 2, 3, 4, 5])
#     @lt x LocationType("New", "HDF5", [], [], 1024)
#     println("done")
# end

#@testset "Simple annotation with Block" begin
#    y = Future()
#    num = Future(16)
#
#    y_pa = @pa {mut y Block(1) num Div(num)} wh []
#
#    @pp [y_pa] begin
#        y = fill(1, 16)
#    end
#
#    @pp [y_pa] begin
#        y = y * 2
#        println("hello ", y)
#    end
#
#    evaluate(y)
#end


#@testset "Simple annotation with Stencil" begin
#    x = Future()
#    num = Future(16)
#
#    x_pa = @pa {mut x Stencil(1, 1, 1) num Div(num)} wh []
#   
#    println("after x pa:", x_pa)
#    @pp [x_pa] begin
#        x = fill(1, num)
#    end
#
#    @pp [x_pa] begin
#        for i in 1:size(x, 1)
#            if i == 1
#                x[i] = x[i + 1]
#            elseif i == size(x, 1)
#                x[i] = x[i - 1]
#            else
#                x[i] = x[i - 1] + x[i + 1]
#            end
#        end
#    end
#    println("RIGHT BEFORE EVALUATE")
#    evaluate(x)
#end

# @testset "Simple HDF5" begin
#     using MPI
#     MPI.Init()
#     using HDF5


#     comm = MPI.COMM_WORLD
#     info = MPI.Info()
#     filename = "test_data"
#     ff = h5open(filename, "w", comm, info)

#     Nproc = MPI.Comm_size(comm)
#     myrank = MPI.Comm_rank(comm)
#     M = 10
#     A = fill(myrank, M)  # local data
#     dims = (M, Nproc)    # dimensions of global data

#     # Create dataset
#     dset = create_dataset(ff, "/data", datatype(eltype(A)), dataspace(dims))

#     # Write local data
#     dset[:, myrank + 1] = A
# end


# @testset "Testing pcluster" begin
#     cluster_id = "banyantest"
#     set_cluster_id(cluster_id)
#     config = JobRequest(cluster_id, 2)
#     create_job(config, make_current = true)
# end

@testset "Level 1, 2 BLAS" begin
    j = Job("banyan", 4)

    # Create data
    n = Future(50e6)
    data = Future()

    # Where the data is located
    val(n)
    mem(data, Integer(4 * 50e6), Int)

    # How the data is partitioned
    pt(data, Block())
    mut(data)

    @partitioned data n begin
        data = randn(Integer(n))  # 200M integers
    end

    pt(data, Block())
    mut(data)

    @partitioned data begin
        data .*= 10
    end

    evaluate(data)

    # data = Future()
    # # evaluate(data)

    # pt(data, Block())

    # @partitioned data begin
    #     data .*= 10
    # end

    # @partitioned data begin
    #     data .*= 10
    # end

    # pt(data, [Block(), Block(2)])
    # mut(data)

    # pt(data, [Block()])
    # mut(data)

    # @partitioned data begin
    #     data .*= 10
    # end
    
    # global pending_requests
    # println(pending_requests)
end
