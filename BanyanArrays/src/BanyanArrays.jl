module BanyanArrays

using Banyan

export Array, Vector, Matrix
export read_hdf5, write_hdf5
export ndims, size, length, eltype
export fill, zeros, ones, trues, falses
export map, mapslices, reduce, sort, sortlices

export ReadBlockHDF5,
    ReadGroupHDF5,
    WriteHDF5,
    CopyFromHDF5,
    CopyToHDF5,
    SplitBlock,
    SplitGroup,
    Rebalance,
    Consolidate,
    Shuffle

include("array.jl")
include("pfs.jl")
include("utils_pfs.jl")

end # module
