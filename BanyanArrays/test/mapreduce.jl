include("foo.jl")

@testset "Filling with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res = sum(x)

        res = compute(res)
        @test typeof(res) == Float64
        @test res == 2048
    end
end

@testset "Multiple evaluations apart with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        res1 = compute(sum(x)) # Note: failed here with "key :val_6HTGdt08_idx_0 not found"
        res2 = compute(minimum(x))

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
    use_session_for_testing(scheduling_config_name = scheduling_config) do

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
end

@testset "Simple computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        for _ = 1:8
            # NOTE: This also tests simple writing to and reading from local disk
            x = BanyanArrays.fill(10.0, 2048)
            # x = map(e -> e / 10, x)
            compute_inplace(x)
            # compute_inplace(x)
            sleep(15)
            # NOTE: The only reason why we're not putting `Base.collect(x)` inside the
            # the `@test` is because `@test` will catch exceptions and prevent the
            # session from getting destroyed when an exception occurs and we can't keep
            # running this test if the session ends
            x_collect = compute(x)
            @test x_collect == Base.fill(10.0, 2048)
        end
    end
end

@testset "Computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        # NOTE: This also tests simple writing to and reading from local disk
        x = BanyanArrays.fill(10.0, 2048)
        x = map(e -> e / 10, x)
        compute_inplace(x)
        compute_inplace(x)
        # NOTE: The only reason why we're not putting `Base.collect(x)` inside the
        # the `@test` is because `@test` will catch exceptions and prevent the
        # session from getting destroyed when an exception occurs and we can't keep
        # running this test if the session ends
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        compute_inplace(x)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
    end
end

@testset "Re-computing with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        x = BanyanArrays.fill(10.0, 2048)
        x_sum = reduce(+, x)
        x = map(e -> e / 10, x)
        compute_inplace(x)
        compute_inplace(x_sum)
        x_sum_collect = compute(x_sum)
        @test x_sum_collect == 10.0 * 2048
        compute_inplace(x_sum)
        x_collect = compute(x)
        @test x_collect == Base.fill(1.0, 2048)
        compute(x_sum)
        x_sum_collect = compute(x_sum)
        @test x_sum_collect == 10.0 * 2048
    end
end

@testset "Map with multiple values with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        a = BanyanArrays.fill(10.0, 2048)
        b = BanyanArrays.fill(10.0, 2048)
        c = a + b
        c_sum_collect = compute(sum(c))
        @test c_sum_collect == 2048 * 10.0 * 2
    end
end

@testset "Complex dependency graphs with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        # Here we test more complex dependency graphs where some values are destroyed

        x = BanyanArrays.fill(10.0, 2048)
        y = BanyanArrays.fill(10.0, 2048)
        a = BanyanArrays.fill(10.0, 2048)
        @show a
        @show x
        x += y
        @show x
        @show a
        x += a
        @show x
        @show a
        y_sum_collect = compute(sum(y))
        @test y_sum_collect == 2048 * 10.0
        a = nothing
        x_sum_collect = compute(sum(x))
        @test x_sum_collect == 2048 * 10.0 * 3
        # y = nothing
        # z = x + x
        # z_sum_collect = compute(sum(z))
        # @test z_sum_collect == 2048 * 10.0 * 6
        # x_sum = sum(x)
        # x = nothing
        # x_sum_collect = compute(x_sum)
        # @test x_sum_collect == 2048 * 10.0 * 3
    end
end

@testset "Multiple arrays with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        x1 = BanyanArrays.fill(10.0, 2048)
        x2 = BanyanArrays.fill(10.0, 2048)
        res = map((a, b) -> a * b, x1, x2)

        res_sum_collect = compute(sum(res))
        @test res_sum_collect == 204_800.0
        res_minimum_collect = compute(minimum(res))
        @test res_minimum_collect == 100.0
    end
end

@testset "2D arrays with $scheduling_config for map-reduce" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        x1 = BanyanArrays.fill(1.0, (2048, 2048))
        x2 = BanyanArrays.fill(2.0, (2048, 2048))
        res = map((a, b) -> a * b, x1, x2)
        res += BanyanArrays.ones((2048, 2048))

        res_sum_collect = compute(sum(res))
        @test res_sum_collect == 3.0 * 2048 * 2048
        res_maximum_collect = compute(maximum(res))
        @test res_maximum_collect == 3.0
    end
end

@testset "Communicating between client and executor with $scheduling_config for map-reduce and $with_parallelism parallelism" for scheduling_config in [
    "default scheduling",
    # "parallelism encouraged",
    # "parallelism and batches encouraged",
], (force_parallelism, with_parallelism) in [(true, "with"), (false, "without"), (true, "with")]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        # Using x1

        x1 = convert(BanyanArrays.Array, [Foo(string(i)) for i in 1:100])
        x_ints = map(f -> parse(Int64, f.x), x1; force_parallelism=force_parallelism)

        @test first(compute(x1)).x == "1"
        @test first(compute(x_ints)) == 1

        x_foos = map(f -> Foo(f.x * "100"), x1; force_parallelism=force_parallelism)
        @test first(compute(x_foos)).x == "1100"

        x_new_foos = map(new_foo, x1; force_parallelism=force_parallelism)
        @test first(compute(x_new_foos)).x == "1100"

        # Making new "x"s

        x2 = convert(BanyanArrays.Array, [Foo(string(i)) for i in 1:100])
        x2_ints = map(f -> parse(Int64, f.x), x2; force_parallelism=force_parallelism)

        @test first(compute(x2_ints)) == 1
        @test first(compute(x2)).x == "1"

        x3 = convert(BanyanArrays.Array, [Foo(string(i)) for i in 1:100])
        x3_foos = map(f -> Foo(f.x * "100"), x3; force_parallelism=force_parallelism)
        @test first(compute(x3_foos)).x == "1100"

        x4 = convert(BanyanArrays.Array, [Foo(string(i)) for i in 1:100])
        x4_new_foos = map(new_foo, x4; force_parallelism=force_parallelism)
        @test first(compute(x4_new_foos)).x == "1100"
    end
end

@testset "getindex and collect with $scheduling_config and different_partitioning_dims=$different_partitioning_dims" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
], different_partitioning_dims in [true, false]
    use_session_for_testing(scheduling_config_name = scheduling_config) do

        Banyan.investigate_different_partitioning_dims(different_partitioning_dims)

        x = BanyanArrays.fill(1.0, (1000, 100))
        x_vecs = mapslices(v -> [v], x, dims=2)[:, 1]
        bc = BanyanArrays.collect(1:1000)
        res = map(x_vecs, BanyanArrays.collect(1:1000)) do x_vec, i
            length(x_vec) + i
        end
        res_sum_compute = compute(sum(res))
        @test res_sum_compute == sum((1.0 * 100 + i for i in 1:1000))

        x = BanyanArrays.fill(1.0, (1000, 100))
        x_vecs = mapslices(v -> [v], x, dims=2)[:]
        @test length(x_vecs) == 1000
        bc = BanyanArrays.collect(1:length(x_vecs))
        res = map(x_vecs, BanyanArrays.collect(1:1000)) do x_vec, i
            length(x_vec) + i
        end
        res_sum_compute = compute(sum(res))
        @test res_sum_compute == sum((1.0 * 100 + i for i in 1:1000))

        x = BanyanArrays.fill(1.0, (1000, 100))
        x_vecs = mapslices(v -> [v], x, dims=2)[:]
        bc = BanyanArrays.collect(1:length(x_vecs))
        res = map(x_vecs, BanyanArrays.collect(1:1000)) do x_vec, i
            length(x_vec) + i
        end
        compute_inplace(res)
        res_sum_compute = compute(sum(res))
        @test res_sum_compute == sum((1.0 * 100 + i for i in 1:1000))

        x = BanyanArrays.fill(1.0, (10, 100))
        x_vecs = mapslices(v -> [v], x, dims=2)[:]
        bc = BanyanArrays.collect(1:length(x_vecs))
        offloaded() do 
            bucket = readdir("s3")[1]
            mkpath("s3/$bucket/test_getindex_and_collect/")
        end
        res = map(x_vecs, BanyanArrays.collect(1:1000)) do x_vec, i
            if isdir("s3")
                bucket = readdir("s3")[1]
                write("s3/$bucket/test_getindex_and_collect/part$i.txt", string(x_vec))
            end
            0
        end
        compute_inplace(res)
        part1_str = offloaded() do
            read("s3/$(get_cluster_s3_bucket_name())/test_getindex_and_collect/part1.txt", String)
        end
        offloaded() do 
            bucket = readdir("s3")[1]
            rm("s3/$bucket/test_getindex_and_collect/", recursive=true)
        end
        @test part1_str == string(Base.fill(1.0, 100))
    end
end

# @testset "Main worker stuck" begin
#     use_session_for_testing(scheduling_config_name = "default scheduling") do
#         offloaded(distributed=true) do
#             if get_worker_idx() > 1
#                 error("Failure right here")
#             end
#             sync_across()
#         end
#     end
# end

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

#     res_lengths_minimum_collect = Base.collect(minimum(res_lengths))
#     @test res_lengths_minimum_collect == 18

#     # TODO: Support writing string arrays for this to work
#     # This is unnecessary but will cache `res` on disk
#     # compute_inplace(res)
#     # res_collect = Base.collect(res)
#     # @test res_collect == BanyanArrays.fill("hello\nhello\nworld\n", 2048)

#     # TODO: Test this once we implement a merging function for
#     # variable-sized reductions
#     # res_minimum_collect = Base.collect(minimum(res))
#     # @test res_minimum_collect == "hello\nhello\nworld\n"

#     # TODO: Test this once we implement a merging function for
#     # variable-sized reductions
#     # x = BanyanArrays.fill("hi\n", 8)
#     # res = reduce(*, x)

#     # res_collect = Base.collect(res)
#     # @test res_collect == "hi\nhi\nhi\nhi\nhi\nhi\nhi\nhi\n"
# end

# TODO: Test HDF5 from URL and from S3
