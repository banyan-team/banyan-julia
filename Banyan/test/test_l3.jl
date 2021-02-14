function matmul()

    # Create data
    n = future(Int32(15e2))
    m = future(Int32(10e3))
    p = future(Int32(5e3))
    A = future() # n x m
    B = future() # m x p

    # Where the data is located
    val(n)
    val(m)
    val(p)
    mem(A, Int64(evaluate(n) * evaluate(m)), Float64)
    mem(B, Int64(evaluate(m) * evaluate(p)), Float64)

    # How the data is partitioned
    pt(n, Div())
    pt(m, Replicate())
    pt(p, Div())
    pt(A, Block(1))
    pt(B, Block(2))
    pc(Cross(A, B))
    pc(Co(A, n))
    pc(Co(B, p))

    mut(A)
    mut(B)

    @partitioned A B n m p begin
        A = fill(1, (Int64(n), Int64(m)))
        B = fill(2, (Int64(m), Int64(p)))
    end

    C = future()
    mem(C, Int64(evaluate(n) * evaluate(p)), Float64)

    pt(A, Block(1))
    pt(B, Block(2))
    pt(C, [Block(1), Block(2)])
    pc(Cross(A, B))
    pc(Cross((C, 1), (C, 2))) # Redundant but currently required to assert order of splitting
    pc(Equal((C, 1), (C, 2)))
    pc(Co((C, 1), A))
    pc(Co((C, 2), B))

    mut(C)

    @partitioned A B C begin
        C = A * B
    end

    # C_new = future()
    # loc(C_new, C.location)
    # mut(C_new)

    # pt(C, [Block(1), Block(2)])
    # pc(Cross((C, 1), (C, 2)))
    # pt(C_new, BlockMulti([1, 2]))
    
    # @partitioned C C_new begin
    #     C_new = C
    # end
    # C = C_new

    evaluate(C)
end

@testset "Matrix multiplication" begin
    runtest("Matrix Multiplication", j -> begin
        matmul()
    end)
end
