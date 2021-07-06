#include("../src/clusters.jl")
#include("../src/jobs.jl")


# Test `clusters.jl:load_json`
function test_load_json()
    # Test failure if filename is not valid
    @test_throws ErrorException Banyan.load_json("res/Banyanfile.json")
end


@testset "Test loading files" begin
    run("load json") do
        test_load_json()
    end
end
