using Dates
using Distributions

@testset "Black Scholes" begin
    #start_time = now()
    #end_time = now()
    run_with_session("scholes") do session
        size = 512000000  # 256000000
	price = BanyanArrays.fill(4.0, size)
	strike = BanyanArrays.fill(4.0, size)
	t = BanyanArrays.fill(4.0, size)
	rate = BanyanArrays.fill(4.0, size)
	vol = BanyanArrays.fill(4.0, size)

	d1 = map(
		(p, s, t, r, v)->(
			(log(p / s) + (r + v ^ 2 * 0.5) * t) / (v * sqrt(t))
		),
		price,
		strike,
		t,
		rate,
		vol
	)

	d2 = map(
		(d1, v, t)->(
			d1 - (v * sqrt(t))
		),
		d1,
		vol,
		t
	)

	call = map(
		  (d1, d2, p, s, t, r)->(
			(cdf(Normal(), d1) * p) - (cdf(Normal(), d2) * s * exp(-r * t))
		  ),
		  d1,
		  d2,
		  price,
		  strike,
		  t,
		  rate
	)

	call_sum = sum(call)
	res = collect(call_sum)
        #res = collect(res)
	#end = now()
        #@test typeof(res) == Base.Vector{Float64}
        #@test all(v->v==3.999999985812889, res)
    end
    #end_time = now()
    #println(end_time - start_time)
end


#function run_bs(size::Integer)

    # c05 = Float64(3.0)
    # c10 = Float64(1.5)
    # invsqrt2 = 1.0 / sqrt(2.0)

    # # rsig = rate + (vol.^2) * c05
    # rsig = vol .^ 2 # TODO: Fix issue in FutureArray where operands are same
    # rsig = rsig .* c05
    # rsig = rsig .+ rate

    # # rsig

    # # vol_sqrt = vol .* sqrt.(t)
    # vol_sqrt = sqrt.(t)
    # vol_sqrt = vol_sqrt .* vol

    # # d1 = (log.(price ./ strike) + rsig .* t) ./ vol_sqrt
    # d1 = price ./ strike
    # d1 = log.(d1)
    # tmp = rsig .* t
    # d1 = d1 .+ tmp
    # d1 = d1 ./ vol_sqrt

    # # d1

    # d2 = d1 .- vol_sqrt

    # # d2

    # # d1 = c05 .+ c05 .* exp.(d1 .* invsqrt2)
    # # d2 = c05 .+ c05 .* exp.(d2 .* invsqrt2)
    # d1 = d1 * invsqrt2
    # d1 = exp.(d1)
    # d1 = d1 .* c05
    # d1 = d1 .+ c05
    # d2 = d2 .* invsqrt2
    # d2 = exp.(d2)
    # d2 = d2 .* c05
    # d2 = d2 .+ c05

    # # e_rt = exp.((-rate) .* t)
    # e_rt = -rate
    # e_rt = e_rt .* t
    # e_rt = exp.(e_rt)

    # # call = price .* d1 - e_rt .* strike .* d2
    # call = price .* d1
    # tmp = e_rt .* strike
    # tmp = tmp .* d2
    # call = call .- tmp

    # put = e_rt .* strike .* (c10 .- d2) - price .* (c10 .- d1)

    # evaluate(call)
#end
