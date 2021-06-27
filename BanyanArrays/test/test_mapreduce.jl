@testset "MapReduce-style computation" begin
    run_with_job("Filling") do job
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

    run_with_job("Multiple evaluations apart") do job
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = collect(sum(x))
        res2 = collect(minimum(x))

        @test typeof(res1) == Float64
        @test res1 == 2048
        @test typeof(res2) == Float64
        @test res2 == 1.0
    end

    run_with_job("Multiple evaluations together") do job
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = sum(x)
        res2 = minimum(x)

        res1 = collect(res1)
        res2 = collect(res2)
        @test typeof(res1) == Float64
        @test res1 == 2048
        @test typeof(res2) == Float64
        @test res2 == 1.0
    end

    run_with_job("Computing") do job
        # NOTE: This also tests simple writing to and reading from local disk
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        @show typeof(x)
        compute(x)
        compute(x)
        @show typeof(x)
        @test collect(x) == Base.fill(1.0, 2048)
        @show typeof(x)
        compute(x)
        @test collect(x) == Base.fill(1.0, 2048)
        @test collect(x) == Base.fill(1.0, 2048)
    end

    run_with_job("Re-computing") do job
        x = BanyanArrays.fill(10.0, 2048)
        x_sum = reduce(+, x)
        x = map(e -> e / 10, x)
        compute(x)
        compute(x_sum)
        @test collect(x_sum) == 10.0 * 2048
        compute(x_sum)
        @test collect(x) == Base.fill(1.0, 2048)
        collect(x_sum)
    end

    run_with_job("Multiple arrays") do job
        x1 = BanyanArrays.fill(10.0, 2048)
        x2 = BanyanArrays.fill(10.0, 2048)
        res = map((a, b) ->  a * b, x1, x2)

        @test collect(sum(res)) == 204_800.0
        @test collect(minimum(res)) == 100.0
    end

    run_with_job("2D arrays") do job
        x1 = BanyanArrays.fill(1.0, (2048, 2048))
        x2 = BanyanArrays.fill(2.0, (2048, 2048))
        res = map((a, b) ->  a * b, x1, x2) 
        res += ones((2048, 2048))

        @test collect(sum(res)) == 3.0 * 2048 * 2048
        @test collect(maximum(res)) == 3.0
    end

    run_with_job("String arrays") do job
        x1 = BanyanArrays.fill("hello\n", 2048)
        x2 = deepcopy(x1)
        x3 = BanyanArrays.fill("world\n", 2048)
        res = map(*, x1, x2, x3)

        @test collect(minimum(res)) == "hello\nhello\nworld\n"

        x = BanyanArrays.fill("hi\n", 8)
        res = reduce(*, x)

        @test collect(res) == "hi\nhi\nhi\nhi\nhi\nhi\nhi\nhi\n"
    end

    # TODO: Test HDF5 from URL and from S3
end
