@testset "Simple usage of PNG in $src $format" for (src, format) in [
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
    write_png_files_to_s3(bucket_name, nimages)

    # twice to test caching
    for _ in 1:2
        path = if src == "Internet"
            if format == "path"
                "https://raw.githubusercontent.com/banyan-team/banyan-julia/cailinw/banyan-images/BanyanImages/test/res/test_image.png"
            elseif format == "list of paths"
                [
                    "https://forza-api.tk/img/FERRARI_FXX_K_2014.png",
                    "https://forza-api.tk/img/MERCEDES-AMG_GT_R_2017.png",
                    "https://forza-api.tk/img/ASCARI_KZ1R_2012.png",
                    "https://forza-api.tk/img/JAGUAR_F-TYPE_R_COUPÉ_2015.png"
                ]
            elseif format == "generator"
                (
                    JSON.parsefile(Downloads.download("https://forza-api.tk/"))["image"]
                    for _ in 1:nimages
                )
            end
        elseif src == "S3"
            if format == "path"
                "s3://$bucket_name/test_images/test_image_1.png"
            elseif format == "directory"
                "s3://$bucket_name/test_images/"
            elseif format == "generator"
                ("s3://$bucket_name/test_images/test_image_$i.png" for i in 1:nimages)
            end
        end

        arr = read_png(path)
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

# @testset "Reading/writing PNG $src through $format" for (src, format) in
#     ]
#     # TODO: read
#     # TODO: transform
#     # TODO: write
#     end
# end
