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
        res1 = collect(sum(x)) # Note: failed here with "key :val_6HTGdt08_idx_0 not found"
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

    run_with_job("Simple computing") do job
        for _ in 1:8
            # NOTE: This also tests simple writing to and reading from local disk
            x = BanyanArrays.fill(10.0, 2048)
            # x = map(e -> e / 10, x)
            @show typeof(x)
            compute(x)
            # compute(x)
            @show typeof(x)
            sleep(15)
            @show typeof(x)
            # NOTE: The only reason why we're not putting `collect(x)` inside the
            # the `@test` is because `@test` will catch exceptions and prevent the
            # job from getting destroyed when an exception occurs and we can't keep
            # running this test if the job ends
            x_collect = collect(x)
            @test x_collect == Base.fill(10.0, 2048)
        end
    end

    run_with_job("Computing") do job
        # NOTE: This also tests simple writing to and reading from local disk
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        @show typeof(x)
        compute(x)
        compute(x)
        @show typeof(x)
        # NOTE: The only reason why we're not putting `collect(x)` inside the
        # the `@test` is because `@test` will catch exceptions and prevent the
        # job from getting destroyed when an exception occurs and we can't keep
        # running this test if the job ends
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
        @show typeof(x)
        compute(x)
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
    end

    run_with_job("Re-computing") do job
        x = BanyanArrays.fill(10.0, 2048)
        x_sum = reduce(+, x)
        x = map(e -> e / 10, x)
        compute(x)
        compute(x_sum)
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 10.0 * 2048
        compute(x_sum)
        x_collect = collect(x)
        @show length(x_collect)
        @test x_collect == Base.fill(1.0, 2048)
        collect(x_sum)
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 10.0 * 2048
    end

    run_with_job("Map with multiple values") do job
        a = BanyanArrays.fill(10.0, 2048)
        b = BanyanArrays.fill(10.0, 2048)
        c = a + b
        c_sum_collect = collect(sum(c))
        @test c_sum_collect == 2048 * 10.0 * 2
    end

    run_with_job("Complex dependency graphs") do job
        # Here we test more complex dependency graphs where some values are destroyed

        x = BanyanArrays.fill(10.0, 2048)
        y = BanyanArrays.fill(10.0, 2048)
        a = BanyanArrays.fill(10.0, 2048)
        x += y
        x += a
        y_sum_collect = collect(sum(y))
        @test y_sum_collect == 2048 * 10.0
        a = nothing
        x_sum_collect = collect(sum(x))
        @test x_sum_collect == 2048 * 10.0 * 3
        y = nothing
        z = x + x
        z_sum_collect = collect(sum(z))
        @test z_sum_collect == 2048 * 10.0 * 6
        x_sum = sum(x)
        x=nothing
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 2048 * 10.0 * 3
    end

    run_with_job("Multiple arrays") do job
        x1 = BanyanArrays.fill(10.0, 2048)
        x2 = BanyanArrays.fill(10.0, 2048)
        res = map((a, b) ->  a * b, x1, x2)

        res_sum_collect = collect(sum(res))
        @test res_sum_collect == 204_800.0
        res_minimum_collect = collect(minimum(res))
        @test res_minimum_collect == 100.0
    end

    run_with_job("2D arrays") do job
        x1 = BanyanArrays.fill(1.0, (2048, 2048))
        x2 = BanyanArrays.fill(2.0, (2048, 2048))
        res = map((a, b) ->  a * b, x1, x2) 
        res += BanyanArrays.ones((2048, 2048))

        res_sum_collect = collect(sum(res))
        @test res_sum_collect == 3.0 * 2048 * 2048
        res_maximum_collect = collect(maximum(res))
        @test res_maximum_collect == 3.0
    end

    # TODO: Re-enable this test once we ensure that we can write out small
    # enough datasets without unnecessary batching
    # run_with_job("String arrays") do job
    #     x1 = BanyanArrays.fill("hello\n", 2048)
    #     x2 = deepcopy(x1)
    #     x3 = BanyanArrays.fill("world\n", 2048)
    #     res = map(*, x1, x2, x3)
    #     res_lengths = map(s -> length(s), res)

    #     res_lengths_minimum_collect = collect(minimum(res_lengths))
    #     @test res_lengths_minimum_collect == 18

    #     # TODO: Support writing string arrays for this to work
    #     # This is unnecessary but will cache `res` on disk
    #     # compute(res)
    #     # res_collect = collect(res)
    #     # @test res_collect == BanyanArrays.fill("hello\nhello\nworld\n", 2048)

    #     # TODO: Test this once we implement a merging function for
    #     # variable-sized reductions
    #     # res_minimum_collect = collect(minimum(res))
    #     # @test res_minimum_collect == "hello\nhello\nworld\n"

    #     # TODO: Test this once we implement a merging function for
    #     # variable-sized reductions
    #     # x = BanyanArrays.fill("hi\n", 8)
    #     # res = reduce(*, x)

    #     # res_collect = collect(res)
    #     # @test res_collect == "hi\nhi\nhi\nhi\nhi\nhi\nhi\nhi\n"
    # end

    # TODO: Test HDF5 from URL and from S3
end
