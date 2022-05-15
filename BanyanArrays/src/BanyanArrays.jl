module BanyanArrays

using Banyan,
    Dates,
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

export add_sizes_on_axis

include("array.jl")
include("utils_pfs.jl")
include("pfs.jl")

if Base.VERSION >= v"1.4.2"
    include("precompile.jl")
    _precompile_()
end

end # module
