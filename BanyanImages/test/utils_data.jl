s3_dirs = Dict{String, String}(
    "png" => "test_images",
    "jpg" => "test_images_jpg"
)

function write_png_files_to_s3(bucket_name=get_cluster_s3_bucket_name(), nimages=1)
    global s3_dirs
    s3_dir_png = s3_dirs["png"]
    if length(readdir(S3Path("s3://$bucket_name/$s3_dir_png/", config=Banyan.get_aws_config()))) < nimages
        for i in 1:nimages
            println("Writing image $i to S3")
            rand_image = rand(ImageCore.RGB, 100, 100)
            save("random_test_image.png", rand_image)
            run(`aws s3api put-object --bucket $bucket_name --key $s3_dir_png/test_image_$i.png --body random_test_image.png`)
        end
    end
end

function write_jpg_files_to_s3(bucket_name=get_cluster_s3_bucket_name(), nimages=1)
    global s3_dirs
    s3_dir_jpg = s3_dirs["jpg"]
    if length(readdir(S3Path("s3://$bucket_name/$s3_dir_jpg/", config=Banyan.get_aws_config()))) < nimages
        for i in 1:nimages
            println("Writing image $i to S3")
            rand_image = rand(ImageCore.RGB, 100, 100)
            save("random_test_image.jpg", rand_image)
            run(`aws s3api put-object --bucket $bucket_name --key $s3_dir_jpg/test_image_$i.jpg --body random_test_image.jpg`)
        end
    end
end

function cleanup_s3_test_files(bucket_name=get_cluster_s3_bucket_name())
    global s3_dirs
    # Delete all files in test_images
    for (filetype, s3_dir) in s3_dirs
        for p in s3_list_keys(Banyan.get_aws_config(), bucket_name, "$s3_dir")
            rm(S3Path("s3://$bucket_name/$p", config=Banyan.get_aws_config()), recursive=true)
        end
    end
end