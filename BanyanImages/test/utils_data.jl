s3_dir = "test_images"

function write_png_files_to_s3(bucket_name=get_cluster_s3_bucket_name(), nimages=1)
    if !isdir(S3Path("s3://$bucket_name/$s3_dir/")) && length(readdir(S3Path("s3://$bucket_name/$s3_dir/"))) < nimages
        for i in 1:nimages
            rand_image = rand(ImageCore.RGB, 100, 100)
            save("random_test_image.png", rand_image)
            run(`aws s3api put-object --bucket $s3_bucket --key $s3_dir/test_image_$i.png --body random_test_image.png`)
        end
    end
end

function cleanup_s3_test_files(bucket_name=get_cluster_s3_bucket_name())
    # Delete all files in test_images
    for p in s3_list_keys(Banyan.get_aws_config(), bucket_name, "$s3_dir")
        rm(S3Path("s3://$bucket_name/$p", config=Banyan.get_aws_config()), recursive=true)
    end
end