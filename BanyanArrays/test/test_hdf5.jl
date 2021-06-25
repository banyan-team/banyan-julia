@testset "Loading BanyanArrays from HDF5 datasets" begin
    run_with_job("Load from HDF5 on the Internet") do job
        println(typeof(Base.fill(1.0, 2048)))
        x = BanyanArrays.fill(10.0, 2048)
        println(typeof(x))
        x = map(e -> e / 10, x)
        println(typeof(x))
        res = sum(x)

        res = collect(res)
        @test typeof(res) == Float64
        @test res == 2048
    end

    # run_with_job("Multiple evaluations apart") do job
    #     x = BanyanArrays.fill(10.0, 2048)
    #     x = map(e -> e / 10, x)
    #     res1 = collect(sum(x))
    #     res2 = collect(minimum(x))

    #     @test typeof(res1) == Float64
    #     @test res1 == 2048
    #     @test typeof(res2) == Float64
    #     @test res2 == 1.0
    # end

    # run_with_job("Multiple evaluations together") do job
    #     x = BanyanArrays.fill(10.0, 2048)
    #     x = map(e -> e / 10, x)
    #     res1 = sum(x)
    #     res2 = minimum(x)

    #     res1 = collect(res1)
    #     res2 = collect(res2)
    #     @test typeof(res1) == Float64
    #     @test res1 == 2048
    #     @test typeof(res2) == Float64
    #     @test res2 == 1.0
    # end

    # TODO: Test HDF5 from URL and from S3
end
