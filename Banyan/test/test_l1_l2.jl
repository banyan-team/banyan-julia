@testset "Black Scholes" begin

# TODO: Make BanyanArray a subtype of AbstractArray
struct BanyanArray{T, N}
    data::Future
    size::Future
end

# # Defining a method like the following is helpful if the Future in a data type
# # isn't just the first field of the type that is a Future. In such a case,
# # a custom future function may be defined but users of this data type must
# # be sure to first call future on their data type before calling
# # annotations functions like `mem`/`val`/`pt`/`pc`/`mut`/`loc`/`src`/`dst`.
# Banyan.future(ba::T) where {T<:BanyanArray} = ba.data

function ones(::Type{T}, len::Integer)::BanyanArray{T, 1} where {T<:Number}
    data = future()
    data_size = future((len))
    ty = future(string(T))

    mem(data, len, Float64)
    val(data_size)
    val(ty)

    pt(data, Block())
    pt(data_size, Replicate())
    pt(ty, Replicate())
    mut(data)

    @partitioned data data_size ty begin
        data = ones(eval(Meta.parse(ty)), tuple(data_size...))
    end

    BanyanArray{T, 1}(data, data_size)
end

function *(ba::BanyanArray{T, 1}, other::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
    res_data = future()
    res_size = future()
    target_size = ba.size
    mem(ba, other)

    pt(ba, Block())
    pt(other, Block())
    pt(res_data, Block())
    pt(res_size, Replicate())
    pt(target_size, Replicate())

    pc(Matches(ba, other, res_data))

    @partitioned ba other res_data res_size target_size begin
        res_data = ba * other
        res_size = target_size
    end

    BanyanArray{T, 1}(res, evaluate(ba.size))
end

j = Job("banyan", 4)

size = Integer(64e6)

price = ones(Float64, size)

# price = ones(Float64, size) * 4.0
# strike = ones(Float64, size) * 4.0
# t = ones(Float64, size) * 4.0
# rate = ones(Float64, size) * 4.0
# vol = ones(Float64, size) * 4.0

evaluate(price)

# c05 = Float64(3.0)
# c10 = Float64(1.5)
# invsqrt2 = 1.0 / sqrt(2.0)

# rsig = rate + (vol^2) * c05
# vol_sqrt = vol * sqrt(t)

# d1 = (log(price / strike) + rsig * t) / vol_sqrt
# d2 = d1 - vol_sqrt

# d1 = c05 + c05 * exp(d1 * invsqrt2)
# d2 = c05 + c05 * exp(d2 * invsqrt2)

# e_rt = exp((-rate) * t)

# call = price * d1 - e_rt * strike * d2
# put = e_rt * strike * (c10 - d2) - price * (c10 - d1)

# evaluate(call)
# evaluate(put)

end

# @testset "Black Scholes" begin
#     j = Job("banyan", 4)

#     # Create data
#     n = Future(50e6)
#     data = Future()

#     # Where the data is located
#     val(n)
#     mem(data, Integer(50e6), Float64)

#     # How the data is partitioned
#     pt(n, Div())
#     pt(data, Block())
#     mut(data)

#     @partitioned data n begin
#         data = randn(Integer(n))  # 200M integers
#     end

#     pt(data, Block())
#     mut(data)

#     @partitioned data begin
#         data .*= 10
#     end

#     evaluate(data)
# end