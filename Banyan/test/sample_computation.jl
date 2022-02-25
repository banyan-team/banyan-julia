@testset "Order-preserving hash for samples" begin
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