# @testset "Sample properties for arrays" begin
#     @test Banyan.sample_axes(Base.ones(10,10)) == [1, 2]
#     @test Banyan.sample_axes(Base.ones(10)) == [1]
#     @test Banyan.sample_keys(Base.ones(10,10)) == [1, 2]
#     @test Banyan.sample_keys(Base.ones(10)) == [1]

#     increasing_array_1d = range(1,100,length=100)
#     increasing_array_2d = reshape(range(1,100,length=100), (10,10))
#     string_array_1d = Base.fill("abc", 10)
#     string_array_2d = Base.fill("abc", (10,10))

#     @test Banyan.sample_min(increasing_array_1d, 1) == [1]
#     @test Banyan.sample_max(increasing_array_1d, 1) == [100]
#     @test Banyan.sample_min(increasing_array_2d, 1) == [1]
#     @test Banyan.sample_max(increasing_array_2d, 1) == [10]
#     @test Banyan.sample_min(increasing_array_2d, 2) == [1]
#     @test Banyan.sample_max(increasing_array_2d, 2) == [91]
#     @test Banyan.sample_min(string_array_1d, 1) == orderinghash("abc")
#     @test Banyan.sample_max(string_array_1d, 1) == orderinghash("abc")
#     @test Banyan.sample_min(string_array_2d, 1) == orderinghash("abc")
#     @test Banyan.sample_max(string_array_2d, 1) == orderinghash("abc")
#     @test Banyan.sample_min(string_array_2d, 2) == orderinghash("abc")
#     @test Banyan.sample_max(string_array_2d, 2) == orderinghash("abc")
# end

# @testset "Sample divisions for arrays" begin
#     # Test outliers
#     @test sample_divisions([5,6,7,8], 1) == [
#         (5, 6)
#         (6, 7)
#         (7, 8)
#         (8, 8)
#     ]
#     @test sample_divisions([5,6,7,8,8,8,8], 1) == [(5,8)]
#     @test sample_divisions([5,6,7,8,8,8,20], 1) == [
#         (5, 8)
#         (8, 20)
#     ]
#     @test sample_divisions([5,6,7,8,8,8,8,8,20], 1) == [(5, 20)]

#     # Test 2D arrays
#     @test sample_divisions(reshape(repeat([5,6,7,8,8,8,20],4), (7,4)), 1) == [
#         (5, 8)
#         (8, 20)
#     ]
#     @test sample_divisions(reshape(repeat([5,6,7,8,8,8,20],4), (7,4)), 2) == [
#         (5, 5)
#     ]
#     @test sample_divisions(reshape(repeat([5,6,7,8,8,8,8,8,20],4), (9,4)), 1) == [
#         (5, 20)
#     ]
# end

# @testset "Sample # of groups for arrays" begin
#     @test Banyan.sample_max_ngroups(Base.ones(10), 1) == 1
#     @test Banyan.sample_max_ngroups(Base.ones(10,10), 1) == 1
#     @test Banyan.sample_max_ngroups(Base.ones(10,10), 2) == 1

#     increasing_array_1d = range(1,100,length=100)
#     increasing_array_2d = reshape(range(1,100,length=100), (10,10))
#     @test Banyan.sample_max_ngroups(increasing_array_1d, 1) == 100
#     @test Banyan.sample_max_ngroups(increasing_array_2d, 1) == 10
#     @test Banyan.sample_max_ngroups(increasing_array_2d, 2) == 10
# end

# @testset "Sample percentile for arrays" begin
#     increasing_array_1d = range(1,100,length=100)
#     increasing_array_2d = reshape(range(1,100,length=100), (10,10))

#     @test sample_percentile(increasing_array_1d, 1, 1, 20) == 0.2
#     @test sample_percentile(increasing_array_1d, 1, 80, 100) == 0.21
#     @test sample_percentile(increasing_array_1d, 1, 1, 100) == 1.0
#     @test sample_percentile(increasing_array_1d, 1, 1, 1) == 0.01
#     @test sample_percentile(increasing_array_1d, 1, -100, -10) == 0

#     @test sample_percentile(increasing_array_2d, 1, 1, 20) == 1.0
#     @test sample_percentile(increasing_array_2d, 1, 5, 10) == 0.6
#     @test sample_percentile(increasing_array_2d, 1, 1, 1) == 0.1
#     @test sample_percentile(increasing_array_2d, 1, -100, -10) == 0

#     @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("a"), orderinghash("b")) == 0.5
#     @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("b"), orderinghash("d")) == 0.75
#     @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("e"), orderinghash("f")) == 0.0
#     @test sample_percentile(["a", "b", "c", "d"], 1, orderinghash("d"), orderinghash("d")) == 0.25
# end