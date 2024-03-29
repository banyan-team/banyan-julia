# bucket - name of cluster's s3 bucket
# path - path to write file to in bucket
# download_path - either http(s) link to a file or a local Path indicating the source of the file
function verify_file_in_s3(bucket, path, download_path)
    if !s3_exists(Banyan.get_aws_config(), bucket, path)
        if typeof(download_path) == String &&
           (startswith(download_path, "https://") || startswith(download_path, "http://"))
            Downloads.download(
                download_path,
                S3Path("s3://$(bucket)/$(path)", config = Banyan.get_aws_config()),
            )
        else  # upload local file
            cp(
                Path(download_path),
                S3Path("s3://$(bucket)/$(path)", config = Banyan.get_aws_config()),
            )
        end
    end
end

######################################
# SETUP AND CLEANUP HELPER FUNCTIONS #
######################################

function write_df_to_csv_to_s3(df, filename, filepath, bucket_name, s3path)
    CSV.write(filename, df)
    verify_file_in_s3(bucket_name, s3path, filepath)
end

function write_df_to_parquet_to_s3(df, filename, filepath, bucket_name, s3path)
    Parquet.write_parquet(filename, df)
    verify_file_in_s3(bucket_name, s3path, filepath)
end

function write_df_to_arrow_to_s3(df, filename, filepath, bucket_name, s3path)
    Arrow.write(filename, df)
    verify_file_in_s3(bucket_name, s3path, filepath)
end

function get_local_path_tripdata(s3_path)
    filename = s3_path.segments[end]
    if occursin("csv", filename)
        return "tripdata.csv"
    elseif occursin("parquet", filename)
        return "tripdata.parquet"
    elseif occursin("arrow", filename)
        return "tripdata.arrow"
    end
end

function setup_basic_tests(bucket_name=get_cluster_s3_bucket_name())
    iris_s3_paths = [
        "iris_large.csv",
        "iris_large.parquet",
        "iris_large.arrow",
        "iris_large_dir.csv/",
        "iris_species_info.csv",
        "iris_species_info.parquet",
        "iris_species_info.arrow",
    ]
    bucket_contents = s3_list_keys(Banyan.get_aws_config(), bucket_name)
    to_be_downloaded = [
        iris_s3_path for iris_s3_path in iris_s3_paths if
        # TODO: Use the following when AWSS3.jl supports folders
        # !s3_exists(Banyan.get_aws_config(), bucket_name, iris_s3_path)
        !(iris_s3_path in bucket_contents)
    ]
    if !isempty(to_be_downloaded)
        @info "Downloading $to_be_downloaded"
        println("At start of setup_basic_tests")
        iris_download_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
        iris_species_info_download_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris_species_info.csv"
        iris_local_path = Downloads.download(iris_download_path)
        iris_species_info_local_path = Downloads.download(iris_species_info_download_path)
        df = CSV.read(iris_local_path, DataFrames.DataFrame, stringtype=String)
        df_s = CSV.read(iris_species_info_local_path, DataFrames.DataFrame, stringtype=String)
        # Duplicate df six times and change the species names
        species_list = df[:, :species]
        df = reduce(vcat, [df, df, df, df, df, df])
        for i = 4:18
            append!(species_list, Base.fill("species_$(i)", 50))
        end
        df[:, :species] = species_list
        write_df_to_csv_to_s3(
            df,
            "iris_large.csv",
            p"iris_large.csv",
            bucket_name,
            "iris_large.csv",
        )
        println("After first setup_basic_tests")
        write_df_to_parquet_to_s3(
            df,
            "iris_large.parquet",
            p"iris_large.parquet",
            bucket_name,
            "iris_large.parquet",
        )
        write_df_to_arrow_to_s3(
            df,
            "iris_large.arrow",
            p"iris_large.arrow",
            bucket_name,
            "iris_large.arrow",
        )

        # Write to dir
        df_shuffle = df[shuffle(1:nrow(df)), :]
        chunk_size = 100
        for i = 1:9
            write_df_to_csv_to_s3(
                df_shuffle[((i-1)*chunk_size+1):i*chunk_size, :],
                "iris_large_chunk.csv",
                p"iris_large_chunk.csv",
                bucket_name,
                "iris_large_dir.csv/iris_large_chunk$(i).csv",
            )
        end

        write_df_to_csv_to_s3(
            df_s,
            "iris_species_info.csv",
            p"iris_species_info.csv",
            bucket_name,
            "iris_species_info.csv",
        )
        write_df_to_parquet_to_s3(
            df_s,
            "iris_species_info.parquet",
            p"iris_species_info.parquet",
            bucket_name,
            "iris_species_info.parquet",
        )
        write_df_to_arrow_to_s3(
            df_s,
            "iris_species_info.arrow",
            p"iris_species_info.arrow",
            bucket_name,
            "iris_species_info.arrow",
        )
        println("After all setup_basic_tests")
    end
end

function setup_empty_tests(bucket_name=get_cluster_s3_bucket_name())
    # Write empty dataframe
    empty_df = DataFrames.DataFrame()
    println("At start of setup_empty_tests")
    if !ispath(S3Path("s3://$bucket_name/empty_df.csv", config = Banyan.get_aws_config()))
        write_df_to_csv_to_s3(
            empty_df,
            "empty_df.csv",
            p"empty_df.csv",
            bucket_name,
            "empty_df.csv",
        )
    end
    println("After first setup_empty_tests")
    if !ispath(S3Path("s3://$bucket_name/empty_df.arrow", config = Banyan.get_aws_config()))
        write_df_to_arrow_to_s3(
            empty_df,
            "empty_df.arrow",
            p"empty_df.arrow",
            bucket_name,
            "empty_df.arrow",
        )
    end

    # Write empty dataframe with two columns
    empty_df2 = DataFrames.DataFrame(x = [], y = [])
    if !ispath(S3Path("s3://$bucket_name/empty_df2.csv", config = Banyan.get_aws_config()))
        write_df_to_csv_to_s3(
            empty_df2,
            "empty_df2.csv",
            p"empty_df2.csv",
            bucket_name,
            "empty_df2.csv",
        )
    end
    if !ispath(S3Path("s3://$bucket_name/empty_df2.arrow", config = Banyan.get_aws_config()))
        write_df_to_arrow_to_s3(
            empty_df2,
            "empty_df2.arrow",
            p"empty_df2.arrow",
            bucket_name,
            "empty_df2.arrow",
        )
    end
    println("After all setup_empty_tests")
end

# function setup_nyc_taxi_stress_test(nbytes = "10 GB", bucket_name=get_cluster_s3_bucket_name())
#     nyc_trip_data_120_mb_path = nothing
#     path_in_s3 = nothing
#     num_bytes = Banyan.parse_bytes(nbytes)
#     num_bytes_so_far = 0
#     idx = 0
#     part_names = []
#     while num_bytes_so_far < num_bytes
#         dst_path = S3Path("s3://$bucket_name/nyc_tripdata_large.csv/part$idx.csv", config = Banyan.get_aws_config())
#         if Banyan.INVESTIGATING_SETUP_NYC_TAXI_STRESS_TEST
#             println("In while loop in setup_nyc_taxi_stress_test")
#             @show dst_path
#             @show !s3_exists(Banyan.get_aws_config(), bucket_name, "nyc_tripdata_large.csv/part$idx.csv")
#         end
#         if !s3_exists(Banyan.get_aws_config(), bucket_name, "nyc_tripdata_large.csv/part$idx.csv")
#             if isnothing(nyc_trip_data_120_mb_path)
#                 nyc_trip_data_120_mb_path = Path(download("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"))
#             end
#             cp(isnothing(path_in_s3) ? nyc_trip_data_120_mb_path : path_in_s3, dst_path)
#             path_in_s3 = dst_path
#         end
#         num_bytes_so_far += Banyan.parse_bytes("120 MB")
#         idx += 1
#         push!(part_names, "part$idx.csv")
#     end
#     if Banyan.INVESTIGATING_SETUP_NYC_TAXI_STRESS_TEST
#         println("Outside while loop in setup_nyc_taxi_stress_test")
#         @show part_names
#     end
#     for p in s3_list_keys(Banyan.get_aws_config(), bucket_name, "nyc_tripdata_large.csv/")
#         p_str = string(p)
#         if !any((endswith(p_str, part_name) for part_name in part_names))
#             if Banyan.INVESTIGATING_SETUP_NYC_TAXI_STRESS_TEST
#                 println("In final for loop in setup_nyc_taxi_stress_test with p=$p")
#             end
#             s3_delete(Banyan.get_aws_config(), bucket_name, p)
#         end
#     end
# end

function setup_nyc_taxi_stress_test(;nrows=-1, nbytes="")
    nrows > 0 || !isempty(nbytes) || error("Please specify # of rows or # of bytes")
    offloaded(nrows, nbytes) do nrows, nbytes
        num_files = if !isempty(nbytes)
            cld(
                Banyan.parse_bytes(nbytes),
                Banyan.parse_bytes("36.4 MB")
            )
        else
            cld(nrows, 2_463_931)
        end
        nbytes_descriptor = Banyan.format_bytes(Banyan.parse_bytes("36.4 MB") * num_files)
        nbytes_descriptor_dom = split(nbytes_descriptor, ".")[1]
        nbytes_descriptor_mul = split(nbytes_descriptor, " ")[2]
        p = "s3/$(get_cluster_s3_bucket_name())/nyc_tripdata_yellow_$(nbytes_descriptor_dom)_$(nbytes_descriptor_mul).parquet"
        if !isdir(p)
            mkdir(p)
        end
        p_1 = joinpath(p, "part_1")
        if !isfile(p_1)
            # 36.4 MB with 2_463_931 rows
            Downloads.download("https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet", p_1)
        end
        num_files = num_files
        for i in 2:num_files
            p_i = joinpath(p, "part_$i")
            if !isfile(p_i)
                cp(p_1, p_i)
            end
        end
        replace(p, "s3/" => "s3://")
    end
end

global n_repeats = 10

function setup_stress_tests(bucket_name=get_cluster_s3_bucket_name())
    # Copy n_repeats of each of the four files into S3.
    global n_repeats
    for month in ["01", "02", "03", "04"]
        println("In setup_stress_tests on month=$month")
        # Get source download path and the list of dst S3 paths
        download_path = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-$(month).csv"
        dst_s3_paths = []
        dst_s3_paths_missing = []
        for filetype in ["csv", "parquet", "arrow"]
            for ncopy = 1:n_repeats
                dst_path = "s3://$(bucket_name)/tripdata_large_$(filetype).$(filetype)/tripdata_$(month)_copy$(ncopy).$(filetype)"
                dst_s3_path = S3Path(dst_path, config = Banyan.get_aws_config())
                push!(dst_s3_paths, dst_s3_path)
                if !isfile(dst_s3_path)
                    push!(dst_s3_paths_missing, dst_s3_path)
                end
            end
        end
        # If at least one file doesn't exist in s3, we need to download and write to s3
        if length(dst_s3_paths_missing) > 0
            local_path = Downloads.download(download_path)
            println("Downloaded $local_path")
            df = CSV.read(local_path, DataFrames.DataFrame)
            println("Read $local_path into memory")

            # Write csv to disk if there is at least one missing csv file
            if any(p -> occursin("csv", p), dst_s3_paths_missing)
                CSV.write("tripdata.csv", df)
            end
            # Write parquet to disk if there is at least one missing parquet file
            if any(p -> occursin("parquet", p), dst_s3_paths_missing)
                Parquet.write_parquet("tripdata.parquet", df)
            end
            # Write arrow to disk if there is at least one missing arrow file
            if any(p -> occursin("arrow", p), dst_s3_paths_missing)
                Arrow.write("tripdata.arrow", df)
            end

            # Loop over missing files and upload to s3
            for s3_path in dst_s3_paths_missing
                cp(
                    Path(get_local_path_tripdata(s3_path)),
                    s3_path,
                    config = Banyan.get_aws_config(),
                )
            end
        end
    end
end

function cleanup_tests(bucket_name=get_cluster_s3_bucket_name())
    # Delete all temporary test files that are prepended with "test-tmp__"
    @show bucket_name
    for p in s3_list_keys(Banyan.get_aws_config(), bucket_name)
        if contains(string(p), "test-tmp_")
            # s3_path = S3Path("s3://$bucket_name/$p", config = Banyan.get_aws_config())
            rm(S3Path("s3://$bucket_name/$p", config = Banyan.get_aws_config()), recursive=true)
        end
    end
end

##############################
# HELPER FUNCTIONS FOR TESTS #
##############################

function read_file(path, sample_invalid=false)
    if endswith(path, ".csv")
        return BanyanDataFrames.read_csv(path, sample_invalid=sample_invalid)
    elseif endswith(path, ".parquet")
        return BanyanDataFrames.read_parquet(path, sample_invalid=sample_invalid)
    elseif endswith(path, ".arrow")
        return BanyanDataFrames.read_arrow(path, sample_invalid=sample_invalid)
    else
        error("Invalid file format")
    end
end

function write_file(path, df)
    if endswith(path, ".csv")
        BanyanDataFrames.write_csv(df, path)
    elseif endswith(path, ".parquet")
        BanyanDataFrames.write_parquet(df, path)
    elseif endswith(path, ".arrow")
        BanyanDataFrames.write_arrow(df, path)
    else
        error("Invalid file format")
    end
end

function get_save_path(bucket, df_name, path)
    return "s3://$(bucket)/test-tmp_$(df_name)_$(split(path, "/")[end])"
end