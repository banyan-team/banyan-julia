@testset "Satellite image encoding with $nimages images, $nworkers workers" for (nimages, sample_rate, nworkers) in [
    # (100, 10, 10),
    # (100, 10, 20),
    # (100, 10, 50),  # OK
    # (1000, 100, 50),  # OK
    # (10000, 1000, 50),
    # (10000, 1000, 100),
    # (50000, 5000, 50),
    # (100000, 10000, 50),
    (100000, 10000, 100),
    # (100000, 10000, 150)
]
    for i in 1:1
        use_session_for_testing(nworkers=nworkers, sample_rate=sample_rate) do
            println("Now testing $nimages images on $nworkers workers on session with ID $(Banyan.get_session_id())")
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
            elseif nimages == 1000
                files = (  # 1800
                    IterTools.product(10:11, 1:30, 1:30),
                    (j, k, l) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-10-$j/250m/6/$k/$l.jpg"
                )
            elseif nimages == 10000
                files = (  # 18900
                    IterTools.product(10:10, 10:30, 1:30, 1:30),
                    (i, j, k, l) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-$i-$j/250m/6/$k/$l.jpg"
                )
            elseif nimages == 50000
                files = (  # 56700
                    IterTools.product(10:12, 10:30, 1:30, 1:30),
                    (i, j, k, l) -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-$i-$j/250m/6/$k/$l.jpg"
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
                bucket = get_cluster_s3_bucket_name()
                rm("s3/$bucket/encodings/", recursive=true)
                mkpath("s3/$bucket/encodings/")
                println("Finished making path in S3 bucket $bucket")
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
end 