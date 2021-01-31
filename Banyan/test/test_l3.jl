# @testset "Matrix multiplication" begin
#     j = Job("banyan", 4)

#     # Create data
#     n = Future(15e2)
#     m = Future(10e2)
#     p = Future(5e2)
#     A = Future() # n x m
#     B = Future() # m x p

#     # Where the data is located
#     val(n)
#     val(m)
#     val(p)
#     mem(A, Integer(evaluate(n) * evaluate(m)), Float64)
#     mem(B, Integer(evaluate(m) * evaluate(p)), Float64)

#     # How the data is partitioned
#     pt(n, Div())
#     pt(m, Replicate())
#     pt(p, Div())
#     pt(A, Block(1))
#     pt(B, Block(2))
#     mut(A)
#     mut(B)

#     pc(Cross(A, B))
#     pc(Co(A, n))
#     pc(Co(B, p))

#     @partitioned A B n m p begin
#         # A = randn(Integer(n), Integer(m))
#         # B = randn(Integer(m), Integer(p))
#         A = fill(1, (Integer(n), Integer(m)))
#         B = fill(2, (Integer(m), Integer(p)))
#     end

#     C = Future()
#     mem(C, Integer(evaluate(n) * evaluate(p)), Float64)

#     pt(A, Block(1))
#     pt(B, Block(2))
#     pt(C, [Block(1), Block(2)])
#     mut(C)

#     pc(Cross((C, 1), (C, 2)))
#     pc(Equal((C, 1), (C, 2)))
#     pc(Co((C, 1), A))
#     pc(Co((C, 2), B))

#     @partitioned A B C begin
#         C = A * B
#         println(C[1])
#     end

#     C_new = Future()
#     loc(C_new, C.location)
#     mut(C_new)

#     pt(C, [Block(1), Block(2)])
#     pc(Cross((C, 1), (C, 2)))
#     pt(C_new, BlockMulti([1, 2]))
    
#     @partitioned C C_new begin
#         C_new = C
#     end
#     C = C_new

#     evaluate(C)
# end
