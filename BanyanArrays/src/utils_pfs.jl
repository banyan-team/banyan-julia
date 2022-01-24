Banyan.split_on_executor(src::AbstractArray, d::Integer, i) = selectdim(src, d, i)

function Banyan.merge_on_executor(obj::Vararg{AbstractArray{T,N},M}; key = nothing) where {T,N,M}
    if length(obj) == 1
        obj[1]
    else
        cat(obj...; dims = key)
    end
end