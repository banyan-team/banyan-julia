include("foo.jl")

@testset "Filling with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

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
end

@testset "Multiple evaluations apart with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = collect(sum(x)) # Note: failed here with "key :val_6HTGdt08_idx_0 not found"
        res2 = collect(minimum(x))

        @test typeof(res1) == Float64
        @test res1 == 2048
        @test typeof(res2) == Float64
        @test res2 == 1.0
    end
end

@testset "Multiple evaluations together with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

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
end

@testset "Simple computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        for _ = 1:8
            # NOTE: This also tests simple writing to and reading from local disk
            x = BanyanArrays.fill(10.0, 2048)
            # x = map(e -> e / 10, x)
            @show typeof(x)
            write_to_disk(x)
            # write_to_disk(x)
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
end

@testset "Computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        # NOTE: This also tests simple writing to and reading from local disk
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        @show typeof(x)
        write_to_disk(x)
        write_to_disk(x)
        @show typeof(x)
        # NOTE: The only reason why we're not putting `collect(x)` inside the
        # the `@test` is because `@test` will catch exceptions and prevent the
        # job from getting destroyed when an exception occurs and we can't keep
        # running this test if the job ends
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
        @show typeof(x)
        write_to_disk(x)
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
        x_collect = collect(x)
        @test x_collect == Base.fill(1.0, 2048)
    end
end

@testset "Re-computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        x = BanyanArrays.fill(10.0, 2048)
        x_sum = reduce(+, x)
        x = map(e -> e / 10, x)
        write_to_disk(x)
        write_to_disk(x_sum)
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 10.0 * 2048
        write_to_disk(x_sum)
        x_collect = collect(x)
        @show length(x_collect)
        @test x_collect == Base.fill(1.0, 2048)
        collect(x_sum)
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 10.0 * 2048
    end
end

@testset "Map with multiple values with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        a = BanyanArrays.fill(10.0, 2048)
        b = BanyanArrays.fill(10.0, 2048)
        c = a + b
        c_sum_collect = collect(sum(c))
        @test c_sum_collect == 2048 * 10.0 * 2
    end
end

@testset "Complex dependency graphs with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

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
        x = nothing
        x_sum_collect = collect(x_sum)
        @test x_sum_collect == 2048 * 10.0 * 3
    end
end

@testset "Multiple arrays with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        x1 = BanyanArrays.fill(10.0, 2048)
        x2 = BanyanArrays.fill(10.0, 2048)
        res = map((a, b) -> a * b, x1, x2)

        res_sum_collect = collect(sum(res))
        @test res_sum_collect == 204_800.0
        res_minimum_collect = collect(minimum(res))
        @test res_minimum_collect == 100.0
    end
end

@testset "2D arrays with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        x1 = BanyanArrays.fill(1.0, (2048, 2048))
        x2 = BanyanArrays.fill(2.0, (2048, 2048))
        res = map((a, b) -> a * b, x1, x2)
        res += BanyanArrays.ones((2048, 2048))

        res_sum_collect = collect(sum(res))
        @test res_sum_collect == 3.0 * 2048 * 2048
        res_maximum_collect = collect(maximum(res))
        @test res_maximum_collect == 3.0
    end
end

@testset "Communicating between client and executor with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do

        x1 = convert(BanyanArrays.Array, [Foo(string(i)) for i in 1:100])
        xx = map(f -> parse(Int64, f.x), x1)

        @test first(collect(x1)).x == "1"
        @test first(collect(xx)) == 1

        xx = map(f -> parse(Int64, f.x), x1; force_parallelism=true)
        @show typeof(xx) xx collect(xx)
        @test first(collect(xx)) == 1
    end
end

# TODO: Re-enable this test once we ensure that we can write out small
# enough datasets without unnecessary batching
# @testset "String arrays with $scheduling_config" for scheduling_config in [
#     "default scheduling",
#     "parallelism encouraged",
#     "parallelism and batches encouraged",
# ]
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
