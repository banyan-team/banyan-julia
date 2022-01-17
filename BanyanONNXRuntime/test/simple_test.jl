@testset "Simple model inference" begin
    use_job_for_testing(scheduling_config_name"default scheduling") do
        # Download model
        localpath = Downloads.download("https://github.com/jw3126/ONNXRunTime.jl/raw/main/test/data/increment2x3.onnx")

        # Load model
        model = ONNXRunTime.load_inference(localpath)

        # Create data
        nimages = 120
        data = rand(Float32, (nimages, 2, 3))

        # Call model on data
        mapslices(
            image -> begin
                model(Dict("input" => image))
            end,
            data;
            dims=[2,3]
        )
    end
end