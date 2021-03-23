# TODO: Make BanyanArray a subtype of AbstractArray
struct BanyanArray{T, N}
    data::Future
    size::Future
end

# Defining a method like the following is helpful if the Future in a data type
# isn't just the first field of the type that is a Future. In such a case,
# a custom future function may be defined but users of this data type must
# be sure to first call future on their data type before calling
# annotations functions like `mem`/`val`/`pt`/`pc`/`mut`/`loc`/`src`/`dst`.
Banyan.future(ba::T) where {T<:BanyanArray} = ba.data

function partitioned_vector(ba::BanyanArray{T, 1}) where {T}
    res_data = future()
    res_size = future()
    target_size = ba.size

    mem(ba, res_data)

    pt(ba, Block())
    pt(res_data, Block())
    pt(res_size, Replicate())
    pt(target_size, Replicate())

    pc(Match(ba, res_data))
    pc(Match(res_size, target_size))
    mut(res_data)
    mut(res_size)

    return ba, res_data, res_size, target_size
end

function partitioned_vector_by_vector(ba::BanyanArray{T, 1}, other::BanyanArray{T, 1}) where {T}
    res_data = future()
    res_size = future()
    target_size = ba.size
    mem(ba, other, res_data)

    pt(ba, BlockBalanced())
    pt(other, BlockBalanced())
    pt(res_data, Block())
    pt(res_size, Replicate())
    pt(target_size, Replicate())

    pc(Match(ba, other, res_data))
    pc(Match(res_size, target_size))
    mut(res_data)
    mut(res_size)

    return ba, other, res_data, res_size, target_size
end

function partitioned_vector_by_scalar(ba::BanyanArray{T, 1}, other::T) where {T}
    other = future(other)
    res_data = future()
    res_size = future()
    target_size = ba.size
    # TODO: See whether ba.size is keeping arrays around for longer

    mem(ba, res_data)
    val(other)

    pt(ba, Block())
    pt(res_data, Block())
    pt(other, Replicate())
    pt(res_size, Replicate())
    pt(target_size, Replicate())

    pc(Match(ba, res_data))
    pc(Match(res_size, target_size))
    mut(res_data)
    mut(res_size)

    return ba, other, res_data, res_size, target_size
end

function partitioned_replicated_value(x)
    x = future(x)
    val(x)
    pt(x, Replicate())
    return x
end

function ones(::Type{T}, len::Integer)::BanyanArray{T, 1} where {T<:Number}
    data = future()
    data_size = future((len))
    created_size = future(len) # TODO: Support n-dimensional arrays with Match
    ty = future(T)

    mem(data, len, Float64)
    val(data_size)
    val(created_size)
    val(ty)

    pt(data, Block())
    pt(created_size, Div())
    pt(ty, Replicate())
    mut(data)

    @partitioned data created_size ty begin
        data = ones(ty, created_size)
    end

    BanyanArray{T, 1}(data, data_size)
end

# Binary vector-vector operations
for op in (:+, :-)
    @eval begin
        function Base.$op(ba::BanyanArray{T, 1}, other::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
            ba, other, res_data, res_size, target_size = partitioned_vector_by_vector(ba, other)
            op = partitioned_replicated_value($op)
            @partitioned op ba other res_data res_size target_size begin
                res_data = op(ba, other)
                res_size = target_size
            end

            BanyanArray{T, 1}(res_data, res_size)
        end
    end
end

# Binary vector-scalar operations

for op in (:*, :/)
    @eval begin
        function Base.$op(ba::BanyanArray{T, 1}, other::T)::BanyanArray{T, 1} where {T}
            ba, other, res_data, res_size, target_size = partitioned_vector_by_scalar(ba, other)
            op = partitioned_replicated_value($op)
            @partitioned op ba other res_data res_size target_size begin
                res_data = op(ba, other)
                res_size = target_size
            end

            BanyanArray{T, 1}(res_data, res_size)
        end
    end
end

for op in (:*,)
    @eval begin
        function Base.$op(other::T, ba::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
            ba, other, res_data, res_size, target_size = partitioned_vector_by_scalar(ba, other)
            op = partitioned_replicated_value($op)
            @partitioned op ba other res_data res_size target_size begin
                res_data = op(other, ba)
                res_size = target_size
            end

            BanyanArray{T, 1}(res_data, res_size)
        end
    end
end

# Broadcasting vector-vector operations
function Base.broadcasted(op, ba::BanyanArray{T, 1}, other::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
    ba, other, res_data, res_size, target_size = partitioned_vector_by_vector(ba, other)
    op = partitioned_replicated_value(op)
    @partitioned op ba other res_data res_size target_size begin
        res_data = Base.broadcast(op, ba, other)
        res_size = target_size
    end

    BanyanArray{T, 1}(res_data, res_size)
end

# Broadcasting vector-scalar operations

function broadcasted(op, ba::BanyanArray{T, 1}, other::T)::BanyanArray{T, 1} where {T}
    ba, other, res_data, res_size, target_size = partitioned_vector_by_scalar(ba, other)
    op = partitioned_replicated_value(op)
    @partitioned op ba other res_data res_size target_size begin
        res_data = Base.broadcast(op, ba, other)
        res_size = target_size
    end

    BanyanArray{T, 1}(res_data, res_size)
end

broadcasted(op, ba::BanyanArray{T, 1}, ::Val{V}) where {T, V} = broadcasted(op, ba, T(V))

Base.broadcasted(
    op,
    ba::BanyanArray,
    other,
) where {T} = broadcasted(op, ba, other)

# specialized_wrapper might be Base.literal_pow
Base.broadcasted(
    specialized_wrapper,
    op,
    ba::BanyanArray,
    other,
) = broadcasted(op, ba, other)

function Base.broadcasted(op, other::T, ba::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
    ba, other, res_data, res_size, target_size = partitioned_vector_by_scalar(ba, other)
    op = partitioned_replicated_value(op)
    @partitioned op ba other res_data res_size target_size begin
        res_data = Base.broadcast(op, other, ba)
        res_size = target_size
    end

    BanyanArray{T, 1}(res_data, res_size)
end

function Base.broadcasted(op, ba::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
    ba, res_data, res_size, target_size = partitioned_vector(ba)
    op = partitioned_replicated_value(op)
    @partitioned op ba res_data res_size target_size begin
        res_data = Base.broadcast(op, ba)
        res_size = target_size
    end

    BanyanArray{T, 1}(res_data, res_size)
end

# Unary operators
for op in (:-, :+)
    @eval begin
        function Base.$op(ba::BanyanArray{T, 1})::BanyanArray{T, 1} where {T}
            ba, res_data, res_size, target_size = partitioned_vector(ba)
            op = partitioned_replicated_value($op)
            @partitioned op ba res_data res_size target_size begin
                res_data = op(ba)
                res_size = target_size
            end

            BanyanArray{T, 1}(res_data, res_size)
        end
    end
end

function run_bs(size::Integer)
    price = ones(Float64, size) * 4.0
    strike = ones(Float64, size) * 4.0
    t = ones(Float64, size) * 4.0
    rate = ones(Float64, size) * 4.0
    vol = ones(Float64, size) * 4.0

    price
end
function put_me_back(size::Integer)
    c05 = Float64(3.0)
    c10 = Float64(1.5)
    invsqrt2 = 1.0 / sqrt(2.0)

    # rsig = rate + (vol.^2) * c05
    rsig = vol .^ 2 # TODO: Fix issue in BanyanArray where operands are same
    rsig = rsig .* c05
    rsig = rsig .+ rate

    # rsig

    # vol_sqrt = vol .* sqrt.(t)
    vol_sqrt = sqrt.(t)
    vol_sqrt = vol_sqrt .* vol

    # d1 = (log.(price ./ strike) + rsig .* t) ./ vol_sqrt
    d1 = price ./ strike
    d1 = log.(d1)
    tmp = rsig .* t
    d1 = d1 .+ tmp
    d1 = d1 ./ vol_sqrt

    # d1

    d2 = d1 .- vol_sqrt

    # d2

    # d1 = c05 .+ c05 .* exp.(d1 .* invsqrt2)
    # d2 = c05 .+ c05 .* exp.(d2 .* invsqrt2)
    d1 = d1 * invsqrt2
    d1 = exp.(d1)
    d1 = d1 .* c05
    d1 = d1 .+ c05
    d2 = d2 .* invsqrt2
    d2 = exp.(d2)
    d2 = d2 .* c05
    d2 = d2 .+ c05

    # e_rt = exp.((-rate) .* t)
    e_rt = -rate
    e_rt = e_rt .* t
    e_rt = exp.(e_rt)

    # call = price .* d1 - e_rt .* strike .* d2
    call = price .* d1
    tmp = e_rt .* strike
    tmp = tmp .* d2
    call = call .- tmp

    put = e_rt .* strike .* (c10 .- d2) - price .* (c10 .- d1)

    call
end

@testset "Black Scholes" begin
    runtest("Black Scholes", j -> begin
        size = Integer(1e9)
        call = run_bs(size)
        evaluate(call)
        # evaluate(put)
    end)
end
