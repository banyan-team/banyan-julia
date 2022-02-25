# Creates a job (by calling create_job) and 
# then offloads a function (by calling offloaded) 
# that returns -1, and then tests this. 
# Also tests offloaded functions with parameters. 

@testset "Offload Function" begin
    # cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    use_session_for_testing() do
        @show get_session().worker_memory_used

        res = offloaded() do
            return -1
        end
        @show get_session().worker_memory_used

        res2 = offloaded(()->-1)
        @show get_session().worker_memory_used

        # @test res == -1
        @test res2 == -1

        res3 = offloaded(x -> x* 10, 5)
        @test res3 == 50
        @show get_session().worker_memory_used

        res4 = offloaded(5, 100) do a, b
            a + b
        end
        @test res4 == 105
        @show get_session().worker_memory_used

        offloaded() do
            x = ones(800000000)
            return 0
        end
        @show get_session().worker_memory_used
    end
end

@testset "Offload Latency" begin
    use_session_for_testing() do
        for _ in 1:3
            @time begin
                offloaded() do
                    sleep(15)
                end
            end
        end
        println("Now with distributed!")
        for i in 1:3
            @time begin
                @show offloaded(i, distributed=true) do i
                    sleep(15)
                    "on worker $(get_worker_idx()) with i=$i at time $(now())"
                end
            end
        end
    end
end
