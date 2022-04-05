Banyan.split_on_executor(src::AbstractArray, d::Integer, i) = selectdim(src, d, i)

function Banyan.merge_on_executor(obj::Vararg{AbstractArray{T,N},M}; key = nothing) where {T,N,M}
    if MPI.Initialized() && MPI.Comm_rank(MPI.COMM_WORLD) == 0
        println("in merge_on_executor: $(typeof(obj))")
    end
    if length(obj) == 1
        if MPI.Initialized() && MPI.Comm_rank(MPI.COMM_WORLD) == 0
            println("in merge_on_executor case 1")
        end
        res = obj[1]
        if MPI.Initialized() && MPI.Comm_rank(MPI.COMM_WORLD) == 0
            println("finished merge_on_executor case 1")
        end
        res
    else
        if MPI.Initialized() && MPI.Comm_rank(MPI.COMM_WORLD) == 0
            println("in merge_on_executor case 2")
        end
        res = cat(obj...; dims = key)
        if MPI.Initialized() && MPI.Comm_rank(MPI.COMM_WORLD) == 0
            println("finished merge_on_executor case 1")
        end
        res
    end
end