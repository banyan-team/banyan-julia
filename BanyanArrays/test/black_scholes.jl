using Dates
using Distributions

#start_time = now()
#end_time = now()
@testset "Black Scholes with Banyan with size=$size" for size in [128_000_000, 256_000_000, 512_000_000]
    use_session_for_testing(sample_rate = 128) do

        # NOTE: 64M works but 128M doesn't (and also doesn't batch the I/O)
        price = BanyanArrays.fill(4.0, size)
        strike = BanyanArrays.fill(4.0, size)
        t = BanyanArrays.fill(4.0, size)
        rate = BanyanArrays.fill(4.0, size)
        vol = BanyanArrays.fill(4.0, size)

        d1 = map(
            (p, s, t, r, v) -> ((log(p / s) + (r + v^2 * 0.5) * t) / (v * sqrt(t))),
            price,
            strike,
            t,
            rate,
            vol,
        )

        d2 = map((d1, v, t) -> (d1 - (v * sqrt(t))), d1, vol, t)

        call = map(
            (d1, d2, p, s, t, r) ->
                ((cdf(Normal(), d1) * p) - (cdf(Normal(), d2) * s * exp(-r * t))),
            d1,
            d2,
            price,
            strike,
            t,
            rate,
        )

        call_sum = sum(call)
        call = nothing
        res = compute(call_sum)
        # println(res)
        @show res
        #res = Base.collect(res)
        #end = now()
        #@test typeof(res) == Base.Vector{Float64}
        #@test all(v->v==3.999999985812889, res)
    end
end
#end_time = now()
#println(end_time - start_time)


@testset "Black Scholes without Banyan with size=$size" for size in [128_000, 128_000_000, 512_000_000]
    use_session_for_testing(sample_rate = 128) do

        price = Base.fill(4.0, size)
        strike = Base.fill(4.0, size)
        t = Base.fill(4.0, size)
        rate = Base.fill(4.0, size)
        vol = Base.fill(4.0, size)

        d1 = Base.map(
            (p, s, t, r, v) -> ((log(p / s) + (r + v^2 * 0.5) * t) / (v * sqrt(t))),
            price,
            strike,
            t,
            rate,
            vol,
        )

        d2 = Base.map((d1, v, t) -> (d1 - (v * sqrt(t))), d1, vol, t)

        call = Base.map(
            (d1, d2, p, s, t, r) ->
                ((cdf(Normal(), d1) * p) - (cdf(Normal(), d2) * s * exp(-r * t))),
            d1,
            d2,
            price,
            strike,
            t,
            rate,
        )

        call_sum = Base.sum(call)
        res = compute(call_sum)
        println(res)
        # @test typeof(res) == Base.Vector{Float64}
        # @test all(v->v==3.999999985812889, res)
    end
end

@testset "Black Scholes without Banyan vectorized with size=$size" for size in [
    128_000,
    128_000_000,
    512_000_000,
]
    use_session_for_testing(sample_rate = 128) do

        price = Base.fill(4.0, size)
        strike = Base.fill(4.0, size)
        t = Base.fill(4.0, size)
        rate = Base.fill(4.0, size)
        vol = Base.fill(4.0, size)

        d1 = (log.(price ./ strike) .+ (rate .+ vol .^ 2 .* 0.5) .* t) ./ (vol .* sqrt.(t))


        d2 = d1 .- (vol .* sqrt.(t))

        call =
            (cdf.(Normal(), d1) .* price) - (cdf.(Normal(), d2) .* strike .* exp.(-rate .* t))

        call_sum = Base.sum(call)
        res = compute(call_sum)
        println(res)
    end
end
