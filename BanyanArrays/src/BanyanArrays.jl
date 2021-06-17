module BanyanArrays

using Banyan

export Array, Vector, Matrix
export read_hdf5, write_hdf5
export ndims, size, length, eltype
export fill
export map, mapslices, reduce, sort, sortlices

include("array.jl")

end # module