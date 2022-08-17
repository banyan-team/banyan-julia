# @testset "Simple usage of JPG in $src $format" for (src, format) in [
#     ("Internet", "path"),
#     ("Internet", "list of paths"),
#     ("Internet", "generator"),
#     ("S3", "path"),
#     ("S3", "directory"),
#     ("S3", "generator")
# ]

#     set_session("test_session_id")  # unit test, so doesn't require real session
#     bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
#     nimages = 4
#     image_size = 100 * 100
#     write_jpg_files_to_s3(bucket_name, nimages)

#     # twice to test caching
#     for it in 1:2
#         path = get_test_path(src, format, "jpg", nimages, bucket_name)

#         arr = read_jpg(path; sample_invalid=(it == 1))
#         arr_size_dim1 = size(arr)[1]
#         arr_length = length(arr)

#         if src == "Internet"
#             if format == "path"
#                 @test arr_size_dim1 == 1
#                 # @test arr_length == image_size * 1
#             elseif format == "list of paths"
#                 @test arr_size_dim1 == 4
#                 # @test arr_length == image_size * 4
#             elseif format == "generator"
#                 @test arr_size_dim1 == nimages
#                 # @test arr_length == image_size * nimages
#             end
#         elseif src == "S3"
#             if format == "path"
#                 @test arr_size_dim1 == 1
#             else
#                 @test arr_size_dim1 == nimages
#             end
#             # @test arr_length == image_size * nimages
#         end
#     end
# end

invalid_bool_to_str(metadata_invalid) = metadata_invalid ? "invalid" : "valid"

@testset "Simple JPG image analysis on $nimages images on $loc with $format and add_channelview=$add_channelview with $(invalid_bool_to_str(metadata_invalid)) metadata and $(invalid_bool_to_str(sample_invalid)) sample" for
    (loc, format) in [
        ("Internet", "generator"),
        ("S3", "generator"),
        ("S3", "directory")
    ],
    add_channelview in [true, false],
    metadata_invalid in [true, false],
    sample_invalid in [true, false],
    nimages in [75, 50]
    # TODO: Test exact sample collection and also replicated with batch image computation
    use_session_for_testing(sample_rate = 75) do
        bucket_name = get_cluster_s3_bucket_name()

        path = get_test_path(loc, "generator", "jpg", nimages, bucket_name)
        images = read_jpg(path, add_channelview=add_channelview, sample_invalid=sample_invalid, metadata_invalid=metadata_invalid)

        if add_channelview
            black_values_count = BanyanArrays.mapslices(
                img -> sum(img .== 0),
                images,
                dims=[2, 3, 4]
            )
            black_values_count = compute(black_values_count)
            @test size(black_values_count) == (nimages, 1, 1, 1)
            @test maximum(black_values_count) == ((nimages == 75) ? 46608 : 10860)
            @test minimum(black_values_count) == ((nimages == 75) ? 0 : 3)
            @test size(images) == (nimages, 3, 512, 512)
        else
            @test size(images) == (nimages, 512, 512)
        end
    end
end

@testset "Reading and sampling $nimages $(shuffled ? "shuffled" : "") JPG images on $loc with $format and add_channelview=$add_channelview with $scheduling_config and a maximum of $max_num_bytes bytes for exact sample" for
    scheduling_config in [
        "default scheduling",
        "parallelism encouraged",
        "parallelism and batches encouraged",
    ],
    (loc, format) in [
        ("Internet", "generator"),
        ("S3", "generator"),
        ("S3", "directory")
    ],
    max_num_bytes in [0, 100_000_000_000],
    shuffled in [true, false],
    nimages in [1, 50],
    add_channelview in [true, false]

    get_organization_id()
    use_session_for_testing(scheduling_config_name = scheduling_config, sample_rate = 20) do
        configure_sampling(max_num_bytes_exact=max_num_bytes, always_shuffled=shuffled, for_all_locations=true, default=true)
        exact_sample = max_num_bytes > 0

        invalidate_all_locations()

        bucket_name = get_cluster_s3_bucket_name()
        p = get_test_path(loc, "generator", "jpg", nimages, bucket_name)

        df = read_jpg(p; add_channelview=add_channelview, metadata_invalid=true, invalidate_samples=true)
        sample(df)
        @show max_num_bytes
        @show exact_sample
        @show get_sample_rate(p; add_channelview=add_channelview)

        configure_sampling(p; sample_rate=50)
        @show get_sampling_configs()
        read_jpg(p; add_channelview=add_channelview)
        @show get_sampling_configs()
        @test get_sample_rate(p; add_channelview=add_channelview) == 50
        @test has_metadata(p; add_channelview=add_channelview)
        @test has_sample(p; add_channelview=add_channelview)
        invalidate_metadata(p; add_channelview=add_channelview)
        @test !has_metadata(p; add_channelview=add_channelview)
        @test has_sample(p; add_channelview=add_channelview)
        invalidate_location(p; add_channelview=add_channelview)
        @test !has_metadata(p; add_channelview=add_channelview)
        @test !has_sample(p; add_channelview=add_channelview)

        @show get_sample_rate(p; add_channelview=add_channelview)
        df2 = read_jpg(p; add_channelview=add_channelview)
        @show Banyan.LocationPath(p; add_channelview=add_channelview)
        @show get_sampling_configs()
        @show get_sampling_config(p; add_channelview=add_channelview)
        @show get_sample_rate(p; add_channelview=add_channelview)
        sample(df2)
        @show get_sample_rate(p; add_channelview=add_channelview)
        df2 = read_jpg(p; add_channelview=add_channelview, samples_invalid=true)
        sample(df2)
        @test get_sample_rate(p; add_channelview=add_channelview) == 50
        configure_sampling(sample_rate=75, for_all_locations=true)
        @test get_sample_rate(p; add_channelview=add_channelview) == 50
        df2 = read_jpg(p; add_channelview=add_channelview, metadata_invalid=true)
        sample(df2)
        @test get_sample_rate(p; add_channelview=add_channelview) == 50
        println("Bad get_sample_rate")
        @test get_sample_rate() == 75
        configure_sampling(sample_rate=75, for_all_locations=true)
        @test get_sample_rate(p; add_channelview=add_channelview) == 50
        println("Bad get_sample_rate")
        configure_sampling(sample_rate=75, force_new_sample_rate=true, for_all_locations=true)
        @test get_sample_rate(p; add_channelview=add_channelview) == 75
        @test get_sample_rate() == 75
        df2 = read_jpg(p; add_channelview=add_channelview)
        @test get_sample_rate(p; add_channelview=add_channelview) == 75
        @test get_sample_rate() == 75
        df2 = read_jpg(p; add_channelview=add_channelview, location_invalid=true)
        sample(df2)
        @test has_metadata(p; add_channelview=add_channelview)
        @test has_sample(p; add_channelview=add_channelview)
        @show get_sample_rate(p; add_channelview=add_channelview)
        configure_sampling(p; add_channelview=add_channelview, always_exact=true)
        sample(df2)
    end
end

# @testset "Reading/writing JPG $src through $format" for (src, format) in
#     ]
#     # TODO: read
#     # TODO: transform
#     # TODO: write
#     end
# end
