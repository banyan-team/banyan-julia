s3_dirs = Dict{String, String}(
    "png" => "test_images",
    "jpg" => "test_images_jpg"
)
img_len = 100

function write_png_files_to_s3(bucket_name=get_cluster_s3_bucket_name(), nimages=1)
    global s3_dirs
    s3_dir_png = s3_dirs["png"]
    if length(readdir(S3Path("s3://$bucket_name/$s3_dir_png/", config=Banyan.get_aws_config()))) < nimages
        for i in 1:nimages
            println("Writing image $i to S3")
            rand_image = rand(ImageCore.RGB, img_len, img_len)
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
            rand_image = rand(ImageCore.RGB, img_len, img_len)
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

function get_test_path(src, format, filetype, nimages, bucket_name)
    global s3_dirs
    s3_dir = s3_dirs[filetype]
    path = if src == "Internet"
        if format == "path"
            "https://raw.githubusercontent.com/banyan-team/banyan-julia/cailinw/banyan-images/BanyanImages/test/res/test_image.$filetype"
        elseif format == "list of paths"
            if filetype == "png"
                [
                    "https://forza-api.tk/img/FERRARI_FXX_K_2014.png",
                    "https://forza-api.tk/img/MERCEDES-AMG_GT_R_2017.png",
                    "https://forza-api.tk/img/AUDI_RS6_2009.png",
                    "https://forza-api.tk/img/JAGUAR_F-TYPE_R_COUPÉ_2015.png"
                ]
            elseif filetype == "jpg"
                [
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/1.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/4.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/16.jpg",
                    "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/32.jpg"
                ]
            end
        elseif format == "generator"
            if filetype == "png"
                (
                    1:4,
                    i -> JSON.parsefile(Downloads.download("https://forza-api.tk/"))["image"]
                )
            elseif filetype == "jpg"
                (
                    1:4,
                    i -> "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Terra_CorrectedReflectance_TrueColor/default/2012-07-09/250m/6/13/$i.jpg"
                )
            end
        end
    elseif src == "S3"
        if format == "path"
            "s3://$bucket_name/$s3_dir/test_image_1.$filetype"
        elseif format == "directory"
            "s3://$bucket_name/$s3_dir/"
        elseif format == "generator"
            prefix_path = "s3://$bucket_name/$s3_dir/test_image_"
            (
                Dict{String,Any}(
                    "prefix_path" => prefix_path,
                    "filetype" => filetype
                ),
                1:nimages,
                (path_info, i) -> path_info["prefix_path"] * "$i." * path_info["filetype"]
            )
        end
    end
end

function get_image_size(src, format, filetype, nimages, channelview)
    expected_size = if src == "Internet"
        if format == "path"
            (1, img_len, img_len)
        elseif format == "list of paths"
            if filetype == "png"
                (4, 1080, 1920)
            elseif filetype == "jpg"
                (4, 512, 512)
            end
        elseif format == "generator"
            if filetype == "png"
                (nimages, 1080, 1920)
            elseif filetype == "jpg"
                (nimages, 512, 512)
            end
        end
    elseif src == "S3"
        if format == "path"
            (1, img_len, img_len)
        elseif format == "directory"
            (nimages, img_len, img_len)
        elseif format == "generator"
            (nimages, img_len, img_len)
        end
    end
    if channelview
        expected_size = (expected_size[1], 3, expected_size[2:end]...)
    end
    expected_size
end