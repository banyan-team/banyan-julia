@testset "Remote PNG Source" begin
    set_job("test_job_id")
    # Prepare test by writing file to S3
    s3_bucket = Random.randstring(['a':'z'; '0':'9'], 6)
    s3_create_bucket(Banyan.get_aws_config(), s3_bucket)
    rand_image = rand(ImageCore.RGB, 100, 100)
    save("test_image.png", rand_image)
    run(`aws s3api put-object --bucket $s3_bucket --key test_image.png --body test_image.png`)

    try
        s = RemotePNGSource("s3://$s3_bucket/test_image.png")
        @test s.src_name == "Remote"
        @test s.src_parameters["nimages"] == 1
        @test s.src_parameters["format"] == "png"
        # TODO: test s.total_memory_usage
        println(s.total_memory_usage)
        println(length(rand_image) * sizeof(eltype(rand_image)))
        @test sample(s.sample) == rand_image
    catch e
        print("Clearing s3 files")
        s3_delete(Banyan.get_aws_config(), s3_bucket, "test_image.png")
        s3_delete_bucket(Banyan.get_aws_config(), s3_bucket)
        rethrow()
    end
    print("Clearing s3 files")
    s3_delete(Banyan.get_aws_config(), s3_bucket, "test_image.png")
    s3_delete_bucket(Banyan.get_aws_config(), s3_bucket)
end