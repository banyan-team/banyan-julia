@testset "Simple usage of BanyanArrays" begin
    run_with_job("Filling") do job
        x = fill(10.0, 2048)
        x = map(e -> e/10, x)
        println(collect(sum(x)))
    end
end
