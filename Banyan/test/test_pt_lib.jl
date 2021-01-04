include(script_path = joinpath(@__DIR__, "../res/pt_lib.jl"))


@testset "Replicate" begin
    pt = PartitionType("Replicate", "Replicate", [1], [], 0)
    split("Replicate", "Batches", src, part, pt, idx, npartitions)
    split("Replicate", "Workers", src, part, pt, idx, npartitions, comm::MPI_Comm)
    split("Replicate", "None", nothing, part, pt, idx, npartitions, lt::LocationType)
end

@testset "Block" begin
    pt = PartitionType("Block", "Block", [0], [], 0)
end

@testset "Stencil" begin
    
end