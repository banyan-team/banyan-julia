@testset "Simple usage of BanyanArrays" begin
    run_with_job("Filling") do job
        println(typeof(Base.fill(1.0, 2048)))
        x = BanyanArrays.fill(10.0, 2048)
        println(typeof(x))
        x = map(e -> e / 10, x)
        println(typeof(x))
        res = sum(x)

        res = collect(res)
        @test typeof(res) == Float64
        @test res = 1024
    end
end
