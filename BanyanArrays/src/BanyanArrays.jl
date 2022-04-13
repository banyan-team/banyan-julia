module BanyanArrays

using Banyan,
    LRUCache,
    Memoize,
    MPI,
    Serialization

export Array, Vector, Matrix
export ndims, size, length, eltype
export fill, zeros, ones, trues, falses, collect
export map, mapslices, reduce, sort, sortlices, getindex

export ReadBlockJuliaArray,
    ReadGroupJuliaArray,
    WriteJuliaArray,
    CopyFromJuliaArray,
    CopyToJuliaArray,
    SplitBlock,
    SplitGroup,
    RebalanceArray,
    ConsolidateArray,
    ShuffleArray

include("array.jl")
include("utils_pfs.jl")
include("pfs.jl")

end # module
