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
    # for _ in 1:2
    for _ in 1:1
        path = get_test_path(src, format, "jpg", nimages, bucket_name)

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


@testset "Simple usage of ReadBlockImage $src $format for $filetype files" for (src, format) in [
    ("Internet", "path"),
    ("Internet", "list of paths"),
    ("Internet", "generator"),
    # ("S3", "path"),
    # ("S3", "directory"),
    # ("S3", "generator")
], filetype in ["jpg"]  # ["png", "jpg"]

    set_job("test_job_id")
    bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
    nimages = 4
    image_size = 100 * 100

    if filetype == "png"
        write_png_files_to_s3(bucket_name, nimages)
    elseif filetype == "jpg"
        write_jpg_files_to_s3(bucket_name, nimages)
    end
    

    comm = MPI.COMM_WORLD
    my_rank = MPI.Comm_rank(comm)
    @test my_rank == 0

    # Construct path
    path = get_test_path(src, format, filetype, nimages, bucket_name)

    # Construct files
    if format == "directory"
        files = readdir(S3Path(path, config=Banyan.get_aws_config()))
    elseif format == "generator"
        files = Banyan.to_jl_value_contents(path)
    elseif format == "path"
        files = [path]
        nimages = 1
    else
        files = path
        nimages = 4
    end

    images = ReadBlockImage(
        nothing,
        Dict{}(),
        1,
        1,
        comm,
        "Remote",
        Dict{}(
            "path" => path,
            "files" => files,
            "nimages" => nimages,
            "nbytes" => 0, # Inaccurate value
            "ndims" => 3,
            "size" => 0, # Inaccurate value
            "eltype" => ImageCore.RGB{N0f8},
            "format" => filetype
        ),
    )

    # Get the expected size
    expected_size = get_image_size(src, format, filetype, nimages)
    @test size(images) == expected_size

end