# @testset "Sample properties for data frames" begin
#     df = DataFrames.DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
#     @test Banyan.sample_axes(df) == [1]
#     @test Banyan.sample_keys(df) == ["x", "y"]
#     @test Banyan.sample_min(df, :x) == 1
#     @test Banyan.sample_max(df, :x) == 4
#     @test Banyan.sample_min(df, :y) == orderinghash("a")
#     @test Banyan.sample_max(df, :y) == orderinghash("d")
# end

# @testset "Sample divisions for data frames" begin
#     df1 = DataFrames.DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
#     df2 = DataFrames.DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
#     df3 = DataFrames.DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])

#     # Test df1
#     @test sample_divisions(df1, :x) == [
#         (1, 2)
#         (2, 3)
#         (3, 4)
#         (4, 4)
#     ]
#     sample_divisions_df1_y = sample_divisions(df1, :y)
#     @test length(sample_divisions_df1_y) == 4
#     for (i, division) in enumerate(sample_divisions_df1_y)
#         # Each division is a tuple of min/max values representing the range
#         # [min, max); i.e., inclusive of minimum and exclusive of the maximum.
#         # Minimums and maximums are ignored for the first and final partitions.
#         # In other words, the first partition is not lower-bounded and the
#         # final partition is not upper-bounded.
#         if i < length(sample_divisions_df1_y)
#             @test division[2] == sample_divisions_df1_y[i+1][1]
#         end

#         # The very last range should just be a single "d"
#         if i < length(sample_divisions_df1_y)
#             @test division[1] < division[2]
#         else
#             @test division[1] == division[2]
#         end
#     end

#     # Test df2 and df3
#     for df in [df2, df3]
#         sample_divisions_df_y = sample_divisions(df, :y)
#         @test length(sample_divisions_df_y) == 2
#         for (i, division) in enumerate(sample_divisions_df_y)
#             if i < length(sample_divisions_df_y)
#                 @test division[2] == sample_divisions_df_y[i+1][1]
#             end
#             @test division[1] < division[2]
#         end
#     end

#     # Test outliers
#     @test sample_divisions(DataFrames.DataFrame(:x=>[5,6,7,8]), :x) == [
#         (5, 6)
#         (6, 7)
#         (7, 8)
#         (8, 8)
#     ]
#     @test sample_divisions(DataFrames.DataFrame(:x=>[5,6,7,8,8,8,8]), :x) == [(5,8)]
#     @test sample_divisions(DataFrames.DataFrame(:x=>[5,6,7,8,8,8,20]), :x) == [
#         (5, 8)
#         (8, 20)
#     ]
#     @test sample_divisions(DataFrames.DataFrame(:x=>[5,6,7,8,8,8,8,8,20]), :x) == [(5, 20)]
# end

# @testset "Sample # of groups for data frames" begin
#     df1 = DataFrames.DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
#     df2 = DataFrames.DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
#     df3 = DataFrames.DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])
#     @test Banyan.sample_max_ngroups(df1, :x) == 4
#     @test Banyan.sample_max_ngroups(df1, :y) == 4
#     @test Banyan.sample_max_ngroups(df2, :x) == 2
#     @test Banyan.sample_max_ngroups(df2, :y) == 2
#     @test Banyan.sample_max_ngroups(df3, :x) == 5
#     @test Banyan.sample_max_ngroups(df3, :y) == 2
# end

# @testset "Sample percentile for data frames" begin
#     df1 = DataFrames.DataFrame(:x => [1,2,3,4], :y => ["a", "d", "b", "c"])
#     df2 = DataFrames.DataFrame(:x => [1,2,3,4,4], :y => ["a", "d", "b", "b", "c"])
#     # df2 = DataFrames.DataFrame(:x => [1,2,3,4,5], :y => ["a", "d", "b", "b", "c"])

#     @test sample_percentile(df1, :x, 1, 4) == 1
#     @test sample_percentile(df1, :x, 2, 4) == 0.75
#     @test sample_percentile(df2, :x, 1, 4) == 1
#     @test sample_percentile(df2, :x, 2, 4) == 0.8
# end

# function test_split_divisions_valid(splits, original=nothing)
#     @test length(splits) >= 1
#     for (j, split) in enumerate(splits)
#         @test length(split) >= 1
#         for (i, division) in enumerate(split)
#             @test length(division) == 2
#             if i < length(split)
#                 @test division[2] == split[i+1][1]
#             end
#             @test division[1] <= division[2]
#         end

#         if j < length(splits)
#             @test last(split)[2] == first(splits[j+1])[1]
#         end
#     end

#     if !isnothing(original)
#         @test first(first(splits))[1] >= first(original)[1]
#         @test last(last(splits))[2] <= last(original)[2]
#     end
# end

# @testset "Splitting sampled divisions into $npieces" for npieces in [2, 1, 5]
#     # TOOD: Test `get_partition_idx_from_divisions`

#     df = CSV.read(use_data("csv", "Disk", true), DataFrames.DataFrame)
#     original_divisions = sample_divisions(df, :species)
#     test_split_divisions_valid(Banyan.get_divisions(original_divisions, npieces), original_divisions)
#     # @test get_partition_idx_from_divisions("" , original_divisions)
# end

# @testset "Splitting sampled divisions recursively" begin
#     df = CSV.read(use_data("csv", "Disk", true), DataFrames.DataFrame)
#     original_divisions = sample_divisions(df, :species)
#     test_split_divisions_valid(Banyan.get_divisions(Banyan.get_divisions(original_divisions, 2)[1], 5), original_divisions)
# end