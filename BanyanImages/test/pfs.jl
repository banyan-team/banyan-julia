@testset "ReadBlockPNG" begin
    set_job("test_job_id")
    # Prepare test by writing file to S3
    s3_bucket = Random.randstring(['a':'z'; '0':'9'], 6)
    s3_create_bucket(Banyan.get_aws_config(), s3_bucket)
    rand_image = rand(ImageCore.RGB, 100, 100)
    save("test_image.png", rand_image)
    run(`aws s3api put-object --bucket $s3_bucket --key test_image.png --body test_image.png`)

    comm = MPI.COMM_WORLD

    my_rank = MPI.Comm_rank(comm)
    @test my_rank == 0

    read_png()

end