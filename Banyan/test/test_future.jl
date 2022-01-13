# Test constructing empty future
function test_construct_empty_future()
    fut = Banyan.Future()
    @test fut.value == nothing
    @test fut.mutated == false
    @test Banyan.get_src_name(fut) == "None"
    fut = Banyan.Future(source=Client())
    @test fut.value == nothing
    @test fut.mutated = true
    @test Banyan.get_dst_name(fut) == "None"
end


# Test constructing future from value
function test_construct_future_from_value()
    fut = Banyan.Future([1, 2, 3, 4, 5])
    @test fut.value == [1, 2, 3, 4, 5]
    @test fut.mutated == false
    @test fut.stale == false
    @test Banyan.get_src_name(fut) == "Value"
    @test Banyan.get_dst_name(fut) == "None"
end


# Test constructing future from existing future
function test_construct_future_from_future()
    fut = Banyan.Future([1, 2, 3, 4, 5])
    new_fut = Banyan.Future(fut)
    @test new_fut.value == fut.value
    @test fut.value_id != new_fut.value_id
    # Test when fut is stale
    fut.stale = true
    new_fut = Banyan.Future(fut)
    @test new_fut.value == nothing
end


# Test getting future from value id
function test_get_future_from_value_id()
    data = [1, 2, 3, 4, 5]
    fut = Banyan.Future(data)
    @test_throws KeyError Banyan.get_future(fut.value_id).value
    Banyan.sourced(fut, Client(data))
    @test Banyan.get_future(fut.value_id).value == data
end


# @testset "Test constructing futures" begin
#     job = Banyan.start_session(
#         username = get(ENV, "BANYAN_USERNAME", nothing),
# 	user_id = get(ENV, "BANYAN_USER_ID", nothing),
# 	api_key = get(ENV, "BANYAN_API_KEY", nothing),
# 	cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing),
# 	nworkers = 2,
# 	banyanfile_path = "file://res/Banyanfile.json",
#     )
#     run("construct future") do
#         with_job(job=job) do j
#             test_construct_empty_future()
#             test_construct_future_from_value()
#             test_construct_future_from_future()
#             test_get_future_from_value_id()
#         end
#     end
# end
