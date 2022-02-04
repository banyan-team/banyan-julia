@testset "MapReduce-style computation" begin
    run_with_session("Filling") do session
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res = sum(x)

        res = compute(res)
        @test typeof(res) == Float64
        @test res == 2048
    end

    run_with_session("Multiple evaluations apart") do session
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = compute(sum(x)) # Note: failed here with "key :val_6HTGdt08_idx_0 not found"
        res2 = compute(minimum(x))

        @test typeof(res1) == Float64
        @test res1 == 2048
        @test typeof(res2) == Float64
        @test res2 == 1.0
    end

    run_with_session("Multiple evaluations together") do session
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = sum(x)
        res2 = minimum(x)

        res1 = compute(res1)
        res2 = compute(res2)
        @test typeof(res1) == Float64
        @test res1 == 2048
        @test typeof(res2) == Float64
        @test res2 == 1.0
    end

    run_with_session("Simple computing") do session
        for _ in 1:8
            # NOTE: This also tests simple writing to and reading from local disk
            x = BanyanArrays.fill(10.0, 2048)
            # x = map(e -> e / 10, x)
            write_to_disk(x)
            # write_to_disk(x)
            sleep(15)
            # NOTE: The only reason why we're not putting `collect(x)` inside the
            # the `@test` is because `@test` will catch exceptions and prevent the
            # session from getting destroyed when an exception occurs and we can't keep
            # running this test if the session ends
            x_collect = compute(x)
            @test x_collect == Base.fill(10.0, 2048)
        end
    end

    run_with_session("Computing") do session
        # NOTE: This also tests simple writing to and reading from local disk
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        write_to_disk(x)
        write_to_disk(x)
        # NOTE: The only reason why we're not putting `collect(x)` inside the
        # the `@test` is because `@test` will catch exceptions and prevent the
        # session from getting destroyed when an exception occurs and we can't keep
        # running this test if the session ends
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        write_to_disk(x)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
    end

    run_with_session("Re-computing") do session
        x = BanyanArrays.fill(10.0, 2048)
        x_sum = reduce(+, x)
        x = map(e -> e / 10, x)
        write_to_disk(x)
        write_to_disk(x_sum)
        x_sum_collect = compute(x_sum)
        @test x_sum_collect == 10.0 * 2048
        write_to_disk(x_sum)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        compute(x_sum)
        x_sum_collect = compute(x_sum)
        @test x_sum_collect == 10.0 * 2048
    end

    run_with_session("Map with multiple values") do session
        a = BanyanArrays.fill(10.0, 2048)
        b = BanyanArrays.fill(10.0, 2048)
        c = a + b
        c_sum_collect = compute(sum(c))
        @test c_sum_collect == 2048 * 10.0 * 2
    end

    run_with_session("Complex dependency graphs") do session
        # Here we test more complex dependency graphs where some values are destroyed

        x = BanyanArrays.fill(10.0, 2048)
        y = BanyanArrays.fill(10.0, 2048)
        a = BanyanArrays.fill(10.0, 2048)
        x += y
        x += a
        y_sum_collect = compute(sum(y))
        @test y_sum_collect == 2048 * 10.0
        a = nothing
        x_sum_collect = compute(sum(x))
        @test x_sum_collect == 2048 * 10.0 * 3
        y = nothing
        z = x + x
        z_sum_collect = compute(sum(z))
        @test z_sum_collect == 2048 * 10.0 * 6
        x_sum = sum(x)
        x=nothing
        x_sum_collect = compute(x_sum)
        @test x_sum_collect == 2048 * 10.0 * 3
    end

    run_with_session("Multiple arrays") do session
        x1 = BanyanArrays.fill(10.0, 2048)
        x2 = BanyanArrays.fill(10.0, 2048)
        res = map((a, b) ->  a * b, x1, x2)

        res_sum_collect = compute(sum(res))
        @test res_sum_collect == 204_800.0
        res_minimum_collect = compute(minimum(res))
        @test res_minimum_collect == 100.0
    end

    run_with_session("2D arrays") do session
        x1 = BanyanArrays.fill(1.0, (2048, 2048))
        x2 = BanyanArrays.fill(2.0, (2048, 2048))
        res = map((a, b) ->  a * b, x1, x2) 
        res += BanyanArrays.ones((2048, 2048))

        res_sum_collect = compute(sum(res))
        @test res_sum_collect == 3.0 * 2048 * 2048
        res_maximum_collect = compute(maximum(res))
        @test res_maximum_collect == 3.0
    end

    # TODO: Re-enable this test once we ensure that we can write out small
    # enough datasets without unnecessary batching
    # run_with_session("String arrays") do session
    #     x1 = BanyanArrays.fill("hello\n", 2048)
    #     x2 = deepcopy(x1)
    #     x3 = BanyanArrays.fill("world\n", 2048)
    #     res = map(*, x1, x2, x3)
    #     res_lengths = map(s -> length(s), res)

    #     res_lengths_minimum_collect = collect(minimum(res_lengths))
    #     @test res_lengths_minimum_collect == 18

    #     # TODO: Support writing string arrays for this to work
    #     # This is unnecessary but will cache `res` on disk
    #     # write_to_disk(res)
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
