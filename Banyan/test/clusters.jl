# # TODO: Migrate to using ReTest

# using AWSCore, AWSS3, HTTP

# include("../src/clusters.jl")


# # Test `clusters.jl:load_json`
# function test_load_json()
#     # Test failure if filename is not valid
#     @test_throws ErrorException Banyan.load_json("res/Banyanfile.json")

#     # Test failure if local file does not exist
#     @test_throws ErrorException Banyan.load_json("file://res/filedoesnotexist.json")
#     # Test valid local file can be loaded
#     banyanfile = Banyan.load_json("file://res/Banyanfile.json")
#     @test typeof(banyanfile) <: Dict

#     # Test failure if s3 file does not exist
#     # TODO: Add this
#     # Test valid s3 file can be loaded
#     # TODO: Add this

#     # Test failure if http(s) file does not exist
#     @test_throws HTTP.ExceptionRequest.StatusError Banyan.load_json("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/filedoesnotexist.json")
#     # Test valid http(s) file can be loaded
#     banyanfile = Banyan.load_json("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/Banyanfile.json")
#     @test typeof(banyanfile) <: Dict

# end


# # Test `clusters.jl:load_file`
# function test_load_file()
#     # Test failure if filename is not valid
#     @test_throws ErrorException Banyan.load_file("res/code_dep.jl")

#     # Test failure if local file does not exist
#     @test_throws ErrorException Banyan.load_file("file://res/filedoesnotexist.jl")
#     # Test valid local json file can be loaded
#     f = Banyan.load_file("file://res/Banyanfile.json")
#     @test typeof(f) == String
#     # Test valid local julia file can be loaded
#     f = Banyan.load_file("file://res/code_dep.jl")
#     @test typeof(f) == String

#     # Test failure if s3 file does not exist
#     # TODO: Add this
#     # Test valid s3 file can be loaded
#     # TODO: Add this

#     # Test failure if http(s) file does not exist
#     @test_throws HTTP.ExceptionRequest.StatusError Banyan.load_file("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/filedoesnotexist.json")
#     # Test valid http(s) file can be loaded
#     f = Banyan.load_file("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/Banyanfile.json")
#     @test typeof(f) == String
# end


# # Test `clusters.jl:create_cluster` in cases where it should fail
# function test_create_cluster_failure_cases()
    
# end


# # Test `clusters.jl:create_cluster` in cases where it should succeed
# function test_create_cluster_success_cases()
# end


# # Test `clusters.jl:destroy_cluster`
# function test_destroy_cluster()
# end


# # Test `clusters.jl:get_cluster` and `clusters.jl:get_clusters`
# function test_get_clusters()
# end


# # Test `clusters.jl:get_jobs_for_cluster`
# function test_get_jobs_for_cluster()
# end


# # Test `clusters.jl:assert_cluster_is_ready`
# function test_assert_cluster_is_ready()
# end


# # Test `clusters.jl:update_cluster`
# function test_update_cluster()
# end




# @testset "Test loading files" begin
#     run("load json") do
#         test_load_json()
#     end
#     run("load file") do
#         test_load_file()
#     end
# end




# @testset "Test creating clusters" begin
#     run("create cluster") do
#         test_create_cluster_failure_cases()
#         test_create_cluster_success_cases()
#     end
# end


# @testset "Test destroying clusters" begin
#     run("destroy cluster") do
#         test_destroy_cluster()
#     end
# end


# @testset "Test managing clusters" begin
#     run("get clusters info") do
#         test_get_clusters()
#         test_get_jobs_for_cluster()
#     end
#     run("set cluster status to ready") do
#         test_assert_cluster_is_ready()
#     end
#     run("update cluster") do
#         test_update_cluster()
#     end
# end
