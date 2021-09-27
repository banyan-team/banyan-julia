using DataFrames, CSV

@testset "Sample properties for arrays" begin
    @test Banyan.sample_axes(ones(10,10)) == [1, 2]
    @test Banyan.sample_axes(ones(10)) == [1]
    @test Banyan.sample_keys(ones(10,10)) == [1, 2]
    @test Banyan.sample_keys(ones(10)) == [1]

    increasing_array_1d = range(1,100,length=100)
    increasing_array_2d = reshape(range(1,100,length=100), (10,10))
    string_array_2d = fill("abc", (10,10))

    @test Banyan.sample_min(increasing_array_1d, 1) == 1
    @test Banyan.sample_max(increasing_array_1d, 1) == 100
    @test Banyan.sample_min(increasing_array_2d, 1) == 1
    @test Banyan.sample_max(increasing_array_2d, 1) == 10
    @test Banyan.sample_min(increasing_array_2d, 2) == 1
    @test Banyan.sample_max(increasing_array_2d, 2) == 91
    @test Banyan.sample_min(string_array_2d, 1) == orderinghash("abc")
    @test Banyan.sample_max(string_array_2d, 1) == orderinghash("abc")
    @test Banyan.sample_min(string_array_2d, 2) == orderinghash("abc")
    @test Banyan.sample_max(string_array_2d, 2) == orderinghash("abc")
end

@testset "Sample properties for data frames" begin
    df = DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
    @test Banyan.sample_axes(df) == [1]
    @test Banyan.sample_keys(df) == [:x, :y]
    @test Banyan.sample_minimum(df, :x) == 1
    @test Banyan.sample_maximum(df, :x) == 4
    @test Banyan.sample_minimum(df, :y) == orderinghash("a")
    @test Banyan.sample_maximum(df, :y) == orderinghash("d")
end

@testset "Ordering hash for samples" begin
    increasing_array_1d = range(1,100,length=100)
    increasing_array_1d_smaller = range(-50,49,length=100)
    increasing_array_2d = reshape(range(1,100,length=100), (10,10))
    increasing_array_2d_smaller = reshape(range(-50,49,length=100), (10,10))

    @test orderinghash(10) < orderinghash(10000)
    @test orderinghash(-10) < orderinghash(0)
    @test orderinghash(increasing_array_1d_smaller) < orderinghash(increasing_array_1d)
    @test orderinghash(increasing_array_2d_smaller) < orderinghash(increasing_array_2d)
    @test orderinghash("abcd") < orderinghash("abce")
    @test orderinghash("abcd") < orderinghash("abcdf")
    @test orderinghash("Abcdf") < orderinghash("abcd")
end

@testset "Sample divisions for arrays" begin
    # Test outliers
    @test sample_divisions([5,6,7,8], 1) == [
        (5, 6)
        (6, 7)
        (7, 8)
        (8, 8)
    ]
    @test sample_divisions([5,6,7,8,8,8,8], 1) == [(5,8)]
    @test sample_divisions([5,6,7,8,8,8,20], 1) == [
        (5, 8)
        (8, 20)
    ]
    @test sample_divisions([5,6,7,8,8,8,8,8,20], 1) == [(5, 20)]

    # Test 2D arrays
    @test sample_divisions(reshape(repeat([5,6,7,8,8,8,20],4), (7,4)), 1) == [
        (5, 8)
        (8, 20)
    ]
    @test sample_divisions(reshape(repeat([5,6,7,8,8,8,20],4), (7,4)), 2) == [
        (5, 5)
    ]
    @test sample_divisions(reshape(repeat([5,6,7,8,8,8,8,8,20],4), (9,4)), 1) == [
        (5, 20)
    ]
end

@testset "Sample divisions for data frames" begin
    df1 = DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
    df2 = DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
    df3 = DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])

    # Test df1
    @test sample_divisions(df1, :x) == [
        (5, 6)
        (6, 7)
        (7, 8)
        (8, 8)
    ]
    sample_divisions_df1_y = sample_divisions(df1, :y)
    @test length(sample_divisions_df1_y) == 4
    for (i, division) in enumerate(sample_divisions_df1_y)
        if i < length(sample_divisions_df1_y)
            @test division[2] == sample_divisions_df1_y[i+1][1]
        end
        @test division[1] < division[2]
    end

    # Test df2 and df3
    for df in [df2, df3]
        sample_divisions_df_y = sample_divisions(df, :y)
        @test length(sample_divisions_df_y) == 2
        for (i, division) in enumerate(sample_divisions_df_y)
            if i < length(sample_divisions_df_y)
                @test division[2] == sample_divisions_df_y[i+1][1]
            end
            @test division[1] < division[2]
        end
    end

    # Test outliers
    @test sample_divisions(DataFrame(:x=>[5,6,7,8]), :x) == [
        (5, 6)
        (6, 7)
        (7, 8)
        (8, 8)
    ]
    @test sample_divisions(DataFrame(:x=>[5,6,7,8,8,8,8]), :x) == [(5,8)]
    @test sample_divisions(DataFrame(:x=>[5,6,7,8,8,8,20]), :x) == [
        (5, 8)
        (8, 20)
    ]
    @test sample_divisions(DataFrame(:x=>[5,6,7,8,8,8,8,8,20]), :x) == [(5, 20)]
end

@testset "Sample # of groups for arrays" begin
    @test Banyan.sample_max_ngroups(ones(10,10), 1) == 10
    @test Banyan.sample_max_ngroups(ones(10,10), 2) == 10

    increasing_array_2d = reshape(range(1,100,length=100), (10,10))
    @test Banyan.sample_max_ngroups(ones(10,10), 1) == 10
    @test Banyan.sample_max_ngroups(ones(10,10), 2) == 10
end

@testset "Sample # of groups for data frames" begin
    df1 = DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
    df2 = DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
    df2 = DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])
    @test Banyan.sample_max_ngroups(df1, :x) == 4
    @test Banyan.sample_max_ngroups(df1, :y) == 4
    @test Banyan.sample_max_ngroups(df2, :x) == 2
    @test Banyan.sample_max_ngroups(df2, :y) == 2
    @test Banyan.sample_max_ngroups(df3, :x) == 5
    @test Banyan.sample_max_ngroups(df3, :y) == 2
end

@testset "Sample percentile for arrays" begin
    increasing_array_1d = range(1,100,length=100)
    increasing_array_2d = reshape(range(1,100,length=100), (10,10))

    @test sample_percentile(increasing_array_1d, 1, 1, 20) == 0.2
    @test sample_percentile(increasing_array_1d, 1, 80, 100) == 0.21
    @test sample_percentile(increasing_array_1d, 1, 1, 100) == 1.0
    @test sample_percentile(increasing_array_1d, 1, 1, 1) == 0.01
    @test sample_percentile(increasing_array_1d, 1, -100, -10) == 0

    @test sample_percentile(increasing_array_2d, 1, 1, 20) == 1.0
    @test sample_percentile(increasing_array_2d, 1, 5, 10) == 0.6
    @test sample_percentile(increasing_array_2d, 1, 1, 1) == 0.1
    @test sample_percentile(increasing_array_2d, 1, -100, -10) == 0

    @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("a"), orderinghash("b")) == 0.5
    @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("b"), orderinghash("d")) == 0.75
    @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("e"), orderinghash("f")) == 0.0
    @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("d"), orderinghash("d")) == 0.25
end

@testset "Sample percentile for data frames" begin
    df1 = DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
    df2 = DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
    df2 = DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])

    @test sample_percentile(df1, :x, 1, 4) == 1
    @test sample_percentile(df1, :x, 2, 4) == 0.75
    @test sample_percentile(df2, :x, 1, 4) == 1
    @test sample_percentile(df2, :x, 2, 4) == 0.8
end

function is_split_divisions_valid(splits, original=nothing)
    @test length(splits) >= 1
    for (j, split) in enumerate(splits)
        @test length(split) >= 1
        for (i, division) in enumerate(split)
            @test length(division) == 2
            if i < length(split)
                @test division[2] == split[i+1][1]
            end
            @test division[1] < division[2]
        end

        if j < length(splits)
            @test last[split][2] == first(splits[j+1])[1]
        end
    end

    if !isnothing(original)
        @test first(first(splits))[1] >= first(original)[1]
        @test last(last(splits))[2] <= last(original)[2]
    end
end

@testset "Splitting sampled divisions" begin
    # TOOD: Test `get_partition_idx_from_divisions`

    df = CSV.read(use_data("iris_in_a_file.csv", "Disk"), DataFrame)
    original_divisions = sample_divisions(df, :species, 2)
    @test is_split_divisions_valid(get_divisions(original_divisions, 2), original_divisions)
    @test is_split_divisions_valid(get_divisions(get_divisions(original_divisions, 2)[1], 5), original_divisions)

    # @test get_partition_idx_from_divisions("" , original_divisions)
end