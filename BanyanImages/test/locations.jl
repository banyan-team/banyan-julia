@testset "Remote PNG Source for $remotepath" for remotepath in [
    "single file"
]
    # Prepare test by writing file to S3
    s3_bucket = Random.randstring(['a':'z'; '0':'9'], 6)
    s3_create_bucket(Banyan.get_aws_config(), s3_bucket)
    save("test_image.png", rand(ImageCore.RGB, 100, 100))
    run(`aws s3api put-object --bucket $s3_bucket --key test_image.png --body test_image.png`)

    try
        s = RemotePNGSource("s3://$s3_bucket/test_image.png")
        # @test s.src_parameters["path"]
        # TODO:add asserts
    catch e
        print("Clearing s3 files")
        s3_delete(Banyan.get_aws_config(), s3_bucket, "test_image.png")
        s3_delete_bucket(Banyan.get_aws_config(), s3_bucket)
        rethrow()
    end
end