Banyan.split_on_executor(src::AbstractArray, d::Int64, i::Int64) = selectdim(src, d, i)

function Banyan.merge_on_executor(obj::Base.Vector{AbstractArray{T,N}}; key::Int64 = 1) where {T,N}
    if length(obj) == 1
        obj[1]
    else
        cat(obj...; dims = key)
    end
end