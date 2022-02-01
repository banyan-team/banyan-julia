@testset "Simple model inference" begin
    use_session_for_testing(scheduling_config_name = "default scheduling") do
        # Get model path
        model_path = "https://github.com/jw3126/ONNXRunTime.jl/raw/main/test/data/increment2x3.onnx"

        # Load model
        model = BanyanONNXRunTime.load_inference(model_path)

        # Create data
        data = BanyanArrays.ones(Float32, (120, 2, 3))

        # Call model on data
        res = model(Dict("input" => data))["output"]
        println("RES 1: ", res)
        res = compute(res)
        println("RES 2: ", res)

        res_size = size(res)
        @test res_size == (120, 2, 3)
        @show res
        # TODO: Test that data is incremented by 1
    end
end