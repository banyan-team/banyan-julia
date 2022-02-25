module BanyanArrays

using Banyan,
    MPI,
    Serialization

export Array, Vector, Matrix
export read_hdf5, write_hdf5
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
    Rebalance,
    Consolidate,
    Shuffle

include("array.jl")
include("utils_pfs.jl")
include("pfs.jl")

end # module
