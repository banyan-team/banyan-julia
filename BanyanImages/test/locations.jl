@testset "Remote Image Source for $format with metadata_invalid=$metadata_invalid and sample_invalid=$sample_invalid" for format in [
    "png", "jpg"
], metadata_invalid in [
    true, false
], sample_invalid in [
    true, false
]
    set_session("test_session_id")

    # Prepare test by writing file to S3
    bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
    nimages = 4
    image_size = 100 * 100
    path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/cailinw/banyan-images/BanyanImages/test/res/test_image.$format"

    s = RemoteImageSource(path; metadata_invalid=metadata_invalid, sample_invalid=sample_invalid)
    @test s.src_name == "Remote"
    @test s.nimages == 1
    @test s.format == format
    @test s.total_memory_usage == sizeof(ImageCore.RGB{N0f8}) * image_size  # exact sample
    @test s.nbytes == sizeof(ImageCore.RGB{N0f8}) * image_size
    @test s.ndims == 3
    @test s.size == (1, sqrt(image_size), sqrt(image_size))
    @test s.eltype == ImageCore.RGB{N0f8}
    @test size(sample(s.sample)) == (1, sqrt(image_size), sqrt(image_size))  # exact sample

end