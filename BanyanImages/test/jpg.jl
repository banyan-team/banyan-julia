@testset "Simple usage of JPG in $src $format" for (src, format) in [
    ("Internet", "path"),
    ("Internet", "list of paths"),
    ("Internet", "generator"),
    ("S3", "path"),
    ("S3", "directory"),
    ("S3", "generator")
]

    set_session("test_session_id")  # unit test, so doesn't require real session
    bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
    nimages = 4
    image_size = 100 * 100
    write_jpg_files_to_s3(bucket_name, nimages)

    # twice to test caching
    for it in 1:2
        path = get_test_path(src, format, "jpg", nimages, bucket_name)

        arr = read_jpg(path; sample_invalid=(it == 1))
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

# , metadata_invalid in [
#     true, false
# ], sample_invalid in [
#     true, false
# ]

@testset "Simple image analysis on JPG with add_channelview=$add_channelview" for add_channelview in [true, false]
    use_session_for_testing(sample_rate = 75) do
        bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
        nimages = 75

        path = get_test_path("Internet", "generator", "jpg", nimages, bucket_name)
        images = read_jpg(path, add_channelview=add_channelview, sample_invalid=true, metadata_invalid=true)

        if add_channelview
            black_values_count = BanyanArrays.mapslices(
                img -> sum(img .== 0),
                images,
                dims=[2, 3, 4]
            )
            black_values_count = compute(black_values_count)
            @show black_values_count
            @show size(black_values_count)
            @show eltype(black_values_count)
            @test size(images) == (nimages, 3, 512, 512)
        else
            @test size(images) == (nimages, 512, 512)
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
