@testset "Satellite image encoding with $nimages images, $nworkers workers" for (nimages, sample_rate, nworkers) in [
    (100, 10, 10),
    (100, 10, 20),
    (100000, 10000, 50),
    (100000, 10000, 100),
    (100000, 10000, 150)
]
    use_session_for_testing(nworkers=nworkers, sample_rate=sample_rate) do
        # Get model path
        model_path = "https://github.com/banyan-team/banyan-julia/raw/v22.02.13/BanyanONNXRunTime/test/res/image_compression_model.onnx"

        # Load model
        model = BanyanONNXRunTime.load_inference(model_path, dynamic_axis=true)

        # Load data
        files = nothing
        if nimages == 100
            files = (  # 100
                IterTools.product(1:10, 1:10),
                (i, j) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/$i/$j.jpg"
            )
        elseif  nimages == 100000
            files = (  # 113400
                IterTools.product(2011:2012, 10:12, 10:30, 1:30, 1:30),
                (y, i, j, k, l) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/$y-$i-$j/250m/6/$k/$l.jpg"
            )
        else
            error("Cannot test with $nimages number of images.")
        end
        data = BanyanImages.read_jpg(files; add_channelview=true)  # Specify `add_channelview` to add a dimension for the RGB channels
        data = BanyanArrays.map(x -> float(x), data)

        # Call model on data
        res = model(Dict("input" => data))["output"]

        # Create path in S3 to store the encodings
        offloaded() do
            configure(user_id="7f812fbd2dd0941d8a12abc56c8d7ddb", api_key="8066d203223d2adbd9a668b16339ce7c")
            bucket = get_cluster_s3_bucket_name()
            rm("s3/$bucket/encodings/", recursive=true)
            mkpath("s3/$bucket/encodings/")
        end

        # Write each image encoding to a different file in Amazon S3
        res_vecs = mapslices(img -> [img], res, dims=2)[:]
        bc = BanyanArrays.collect(1:length(res_vecs))
        res = map(res_vecs, bc) do img_vec, i
            if isdir("s3")
                bucket = get_cluster_s3_bucket_name()
                write("s3/$bucket/encodings/part$i.txt", string(img_vec))
            end
            0
        end

        @time compute_inplace(res)
    end
end 