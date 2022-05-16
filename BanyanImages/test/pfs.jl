# @testset "Simple usage of ReadBlockImage $src $format for $filetype files with add_channelview=$add_channelview" for (src, format, filetype) in [
#     ("Internet", "path", "png"),
#     ("Internet", "path", "jpg"),
#     ("Internet", "list of paths", "jpg"),
#     ("Internet", "generator", "jpg"),
# ], add_channelview in [true, false]

#     # Create the /efs directory that would exist on the cluster
#     if !isdir("efs")
#         mkdir(("efs"))
#     end
#     set_session("test_session_id")
#     bucket_name = get_cluster_s3_bucket_name(ENV["BANYAN_CLUSTER_NAME"])
#     nimages = 4
#     image_size = 100 * 100

#     if filetype == "png"
#         write_png_files_to_s3(bucket_name, nimages)
#     elseif filetype == "jpg"
#         write_jpg_files_to_s3(bucket_name, nimages)
#     end

#     comm = MPI.COMM_WORLD
#     my_rank = MPI.Comm_rank(comm)
#     @test my_rank == 0

#     # Construct path
#     path = get_test_path(src, format, filetype, nimages, bucket_name)

#     # Construct files
#     if format == "directory"
#         files = readdir(S3Path(path, config=Banyan.get_aws_config()))
#         datasize = add_channelview ? (nimages, 3, 100, 100) : (nimages, 100, 100)
#         empty_part_size = add_channelview ? (0, 3, 100, 100) : (0, 100, 100)
#     elseif format == "generator"
#         files = Banyan.to_jl_value_contents(path)
#         datasize = add_channelview ? (nimages, 3, 512, 512) : (nimages, 512, 512)
#         empty_part_size = add_channelview ? (0, 3, 512, 512) : (0, 512, 512)
#     elseif format == "path"
#         files = [path]
#         nimages = 1
#         datasize = add_channelview ? (1, 3, 100, 100) : (1, 100, 100)
#         empty_part_size = add_channelview ? (0, 3, 100, 100) : (0, 100, 100)
#     elseif format == "list of paths"
#         files = path
#         nimages = 4
#         datasize = add_channelview ? (nimages, 3, 512, 512) : (nimages, 512, 512)
#         empty_part_size = add_channelview ? (0, 3, 512, 512) : (0, 512, 512)
#     else
#         error("Test not supported")
#     end
#     dataeltype = add_channelview ? Float64 : ImageCore.RGB{N0f8}

#     images = ReadBlockImage(
#         nothing,
#         Dict{String,Any}(),
#         1,
#         1,
#         comm,
#         "Remote",
#         Dict{String,Any}(
#             "path" => path,
#             "files" => files,
#             "nimages" => nimages,
#             "nbytes" => 0, # Inaccurate value
#             "ndims" => 3,
#             "size" => datasize, # Inaccurate value
#             "eltype" => dataeltype,
#             "empty_sample" => Banyan.to_jl_value_contents(Base.Array{dataeltype}(undef, empty_part_size)),
#             "format" => filetype,
#             "add_channelview" => add_channelview
#         ),
#     )

#     # Get the expected size
#     expected_size = get_image_size(src, format, filetype, nimages, add_channelview)
#     @test size(images) == expected_size

# end