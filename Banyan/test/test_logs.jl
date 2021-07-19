using BanyanArrays

@testset "Test logs" begin
    run_with_job("logs") do job
        size = 256000000
        arr = BanyanArrays.fill(2.0, size)
        res = map(
	    a -> (a * 10),
	    arr
        )
	res_sum = sum(res)
        res_sum = collect(res_sum)
        println(res_sum)
    end
end
