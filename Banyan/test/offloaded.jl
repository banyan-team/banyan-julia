# Add an @testset that creates a job 
# (by calling create_job) and 
# then offloads a function (by calling offloaded) 
# that returns -1, and then 
# test that the call to offloaded returns the -1 

@testset "Offload Function" begin
    # cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    println("before the function")
    use_session_for_testing() do
        println("in the use session")
        res = offloaded() do
            return -1
        end

        res2 = offloaded(()->-1)

        @test res == -1
        @test res2 == -1
    end
end