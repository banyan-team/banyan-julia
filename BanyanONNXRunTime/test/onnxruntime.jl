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
        res = compute(res)

        res_size = size(res)
        @test res_size == (120, 2, 3)
        all_incremented = all(res .== 2)
        @test all_incremented

        model_sample = sample(model)
        res_sample = model_sample(Dict("input" => sample(data)))["output"]
        res_size = size(res_sample)
        @test res_size == (120, 2, 3)
        all_incremented = all(res_sample .== 2)
        @test all_incremented
    end
end

# @testset "Stress test loading images and model inference" begin
#     use_session_for_testing(scheduling_config_name = "default scheduling", nworkers=50) do
#         # Get model path
#         model_path = "https://github.com/banyan-team/banyan-julia/raw/cailinw/onnx-stress/BanyanONNXRunTime/test/res/image_compression_model.onnx"

#         # Load model
#         model = BanyanONNXRunTime.load_inference(model_path, dynamic_axis=true)

#         # Create data
#         files = (
#             IterTools.product(1:10, 1:10),
#             (i, j) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/$i/$j.jpg"
#         )
#         data = BanyanImages.read_jpg(files; add_channelview=true)  # Specify `add_channelview` to add a dimension for the RGB channels
#         data = BanyanArrays.map(x -> float(x), data)

#         # Call model on data
#         res = model(Dict("input" => data))["output"]
#         res = compute(res)

#         res_size = size(res)
#         @show res_size
#     end
# end