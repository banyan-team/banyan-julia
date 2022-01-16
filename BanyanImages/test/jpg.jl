@testset "Simple usage of JPG in $src $format" for (src, format) in [
    ("Internet", "path"),
    ("Internet", "list of paths"),
    ("Internet", "generator"),
    ("S3", "path"),
    ("S3", "directory"),
    ("S3", "generator")
]

    set_job("test_job_id")  # unit test, so doesn't require real job
    bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
    nimages = 4
    image_size = 100 * 100
    write_jpg_files_to_s3(bucket_name, nimages)

    # twice to test caching
    for _ in 1:2
        path = if src == "Internet"
            if format == "path"
                "https://raw.githubusercontent.com/banyan-team/banyan-julia/cailinw/banyan-images/BanyanImages/test/res/test_image.jpg"
            elseif format == "list of paths"
                [
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/1.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/4.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/16.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/32.jpg"
                ]
            elseif format == "generator"
                (
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/$i.jpg"
                    for i in 1:nimages  # NOTE: max ~70
                )
            end
        elseif src == "S3"
            if format == "path"
                "s3://$bucket_name/test_images_jpg/test_image_1.jpg"
            elseif format == "directory"
                "s3://$bucket_name/test_images_jpg/"
            elseif format == "generator"
                ("s3://$bucket_name/test_images_jpg/test_image_$idx.jpg" for idx in 1:nimages)
            end
        end

        arr = read_jpg(path)
        arr_size_dim1 = size(arr)[1]
        arr_length = length(arr)

        if src == "Internet"
            if format == "path"
                @test arr_size_dim1 == 1
                # @test arr_length == image_size * 1
            elseif format == "list of paths"
                @test arr_size_dim1 == 4
                # @test arr_length == image_size * 4
            elseif format == "generator"
                @test arr_size_dim1 == nimages
                # @test arr_length == image_size * nimages
            end
        elseif src == "S3"
            if format == "path"
                @test arr_size_dim1 == 1
            else
                @test arr_size_dim1 == nimages
            end
            # @test arr_length == image_size * nimages
        end
    end
end

# @testset "Reading/writing JPG $src through $format" for (src, format) in
#     ]
#     # TODO: read
#     # TODO: transform
#     # TODO: write
#     end
# end
