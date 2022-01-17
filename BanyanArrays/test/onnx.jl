@testset "Simple model inference" begin
    use_job_for_testing(scheduling_config_name = "default scheduling") do
        # Download model
        localpath = Downloads.download("https://github.com/jw3126/ONNXRunTime.jl/raw/main/test/data/increment2x3.onnx")

        # Load model
        model = ONNXRunTime.load_inference(localpath)

        # Create data
        data = BanyanArrays.ones(Float32, (120, 2, 3))

        # Call model on data
        res = BanyanArrays.mapslices(
            # image -> begin
            #     model
            # end,
            identity,
            data;
            dims=[2,3]
        )
        res = collect(res)

        res_size = size(res)
        @test res_size == (120, 2, 3)
        # TODO: Test that data is incremented by 1

        # Call model on data
        res = BanyanArrays.mapslices(
            # image -> begin
            #     model(Dict("input" => image))
            # end,
            sum,
            data;
            dims=[2,3]
        )
        res = collect(res)

        res_size = size(res)
        @test res_size == (120, 1, 1)
        @test res[1] == 6
    end
end