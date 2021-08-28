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

function setup_basic_tests(bucket_name)
    iris_download_path = "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
    iris_species_info_download_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris_species_info.csv"
    iris_local_path = download(iris_download_path)
    iris_species_info_local_path = download(iris_species_info_download_path)
    df = CSV.read(iris_local_path, DataFrames.DataFrame)
    df_s = CSV.read(iris_species_info_local_path, DataFrames.DataFrame)
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

    # Write empty dataframe
    empty_df = DataFrames.DataFrame()
    write_df_to_csv_to_s3(
        empty_df,
        "empty_df.csv",
        p"empty_df.csv",
        bucket_name,
        "empty_df.csv",
    )
    write_df_to_arrow_to_s3(
        empty_df,
        "empty_df.arrow",
        p"empty_df.arrow",
        bucket_name,
        "empty_df.arrow",
    )
end

global n_repeats = 10

function setup_stress_tests(bucket_name)
    global n_repeats
    for month in ["01", "02", "03", "04"]
        for ncopy = 1:n_repeats
            download_path = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-$(month).csv"
            local_path = download(download_path)
            df = CSV.read(local_path, DataFrames.DataFrame)
            write_df_to_csv_to_s3(
                df,
                "tripdata.csv",
                p"tripdata.csv",
                bucket_name,
                "tripdata_large_csv.csv/tripdata_$(month)_copy$(ncopy).csv",
            )
            write_df_to_parquet_to_s3(
                df,
                "tripdata.parquet",
                p"tripdata.parquet",
                bucket_name,
                "tripdata_large_parquet.parquet/tripdata_$(month)_copy$(ncopy).parquet",
            )
            write_df_to_arrow_to_s3(
                df,
                "tripdata.arrow",
                p"tripdata.arrow",
                bucket_name,
                "tripdata_large_arrow.arrow/tripdata_$(month)_copy$(ncopy).arrow",
            )
        end
    end
end

function cleanup_tests(bucket_name)
    # Delete all temporary test files that are prepended with "test-tmp__"
    for obj in s3_list_objects(bucket_name, path_prefix = "test-tmp_")
        s3_delete(bucket_name, obj)
    end
end

##############################
# HELPER FUNCTIONS FOR TESTS #
##############################

function read_file(path)
    if endswith(path, ".csv")
        return BanyanDataFrames.read_csv(path)
    elseif endswith(path, ".parquet")
        return BanyanDataFrames.read_parquet(path)
    elseif endswith(path, ".arrow")
        return BanyanDataFrames.read_arrow(path)
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

#########
# TESTS #
#########

@testset "Filter" begin
    run_with_job("Filtering basic for initial functionality") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_basic_tests(bucket)

        for i = 1:2
            for path in [
                "s3://$(bucket)/iris_large.csv",
                "s3://$(bucket)/iris_large.parquet",
                "s3://$(bucket)/iris_large.arrow",
                "s3://$(bucket)/iris_large_dir.csv",
            ]
                df = read_file(path)

                # Assert that exception gets thrown for parameters that aren't supported
                @test_throws ArgumentError filter(
                    row -> row.sepal_length > 5.0,
                    df;
                    view = true,
                )

                # Filter based on one column
                sub_save_path = get_save_path(bucket, "sub", path)
                sub2_save_path = get_save_path(bucket, "sub2", path)
                if i == 1
                    sub = filter(row -> row.sepal_width == 3.3, df)
                    sub2 = filter(row -> endswith(row.species, "8"), sub)
                    write_file(sub_save_path, sub)
                    write_file(sub2_save_path, sub2)
                else
                    sub = read_file(sub_save_path)
                    sub2 = read_file(sub2_save_path)
                end

                # Collect results
                sub_nrow = nrow(sub)
                sub2_nrow = nrow(sub2)
                sepal_length_sub_sum = round(collect(reduce(+, sub[:, :sepal_length])))
                sepal_length_sub2_sum = round(collect((reduce(+, sub2[:, :sepal_length]))))
                sub2_species = Set(collect(sub2[:, [:species]])[:, :species])

                # Assert
                @test sub_nrow == 36
                @test sepal_length_sub_sum == 217
                @test sub2_nrow == 4
                @test sepal_length_sub2_sum == 26
                @show sample(sub2)
                @test sub2_species == Set(["species_8", "species_18"])


                # Filter based on multiple columns using DataFrameRow syntax
                sub3_save_path = get_save_path(bucket, "sub3", path)
                if i == 1
                    sub3 = filter(
                        row -> (row.species == "versicolor" || row.species == "species_17"),
                        filter(
                            row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0),
                            df,
                        ),
                    )
                    write_file(sub3_save_path, sub3)
                else
                    sub3 = read_file(sub3_save_path)
                end

                # Collect results
                sub3_nrow = nrow(sub3)
                sub3 = sort(collect(sub3))
                sub3_row8 = collect(sub3[8, :])
                sub3_row62 = collect(sub3[62, :])

                # Assert
                @test sub3_nrow == 62
                @test sub3_row8 == [5.5, 2.4, 3.7, 1.0, "versicolor"]
                @test sub3_row62 == [6.8, 2.8, 4.8, 1.4, "versicolor"]

                # Filter based on multiple columns, using cols
                sub4_save_path = get_save_path(bucket, "sub4", path)
                if i == 1
                    sub4 =
                        filter([:petal_length, :petal_width] => (l, w) -> l * w > 12.0, df)
                    write_file(sub4_save_path, sub4)
                else
                    sub4 = read_file(sub4_save_path)
                end

                # Collect results
                sub4_nrow = nrow(sub4)

                sub4_nrow = nrow(sub4)
                sub4_max_petal_length = collect(reduce(max, sub4[:, :petal_length]))
                sub4_valid = collect(
                    reduce(
                        &,
                        map(
                            (l, w) -> l .* w .> 12,
                            sub4[:, :petal_length],
                            sub4[:, :petal_width],
                        ),
                    ),
                )
                sub4 = sort(collect(sub4))
                sub4_row8 = collect(sub4[8, :])
                sub4_row114 = collect(sub4[114, :])

                # Assert
                @test sub4_nrow == 114
                @test sub4_max_petal_length == 6.9
                @test sub4_valid
                @test sub4_row8 == [6.2, 3.4, 5.4, 2.3, "species_15"]
                @test sub4_row114 == [7.9, 3.8, 6.4, 2.0, "virginica"]

                # Filter based on multiple columns, using AsTable
                sub5_save_path = get_save_path(bucket, "sub5", path)
                if i == 1
                    sub5 = filter(
                        AsTable(:) =>
                            iris ->
                                (iris.petal_length % 2 == 0) &&
                                    (iris.sepal_length > 6.0 || iris.petal_width < 0.5),
                        df,
                    )
                    write_file(sub5_save_path, sub5)
                else
                    sub5 = read_file(sub5_save_path)
                end

                # Collect results
                sub5_nrow = nrow(sub5)
                sub5 = sort(collect(sub5))
                sub5_row1 = collect(sub5[1, :])
                sub5_row18 = collect(sub5[18, :])

                # Assert
                @test sub5_nrow == 18
                @test sub5_row1 == [6.1, 2.8, 4.0, 1.3, "species_11"]
                @test sub5_row18 == [7.2, 3.2, 6.0, 1.8, "virginica"]
            end
        end
    end

    run_with_job("Filtering stress for initial functionality") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_stress_tests(bucket)

        for i = 1:2
            for path in [
                "s3://$(bucket)/tripdata_large_csv.csv",
                "s3://$(bucket)/tripdata_large_parquet.parquet",
                "s3://$(bucket)/tripdata_large_arrow.arrow",
            ]
                df = read_file(path)
                @test nrow(df) == 61577490 * n_repeats

                sub_save_path = get_save_path(bucket, "sub", path)
                if i == 1
                    sub = filter(
                        row -> (row.passenger_count != 1 && row.trip_distance > 1.0),
                        df,
                    )
                    write_file(sub_save_path, sub)
                else
                    sub = read_file(sub_save_path)
                end

                # Collect results
                sub_nrow = nrow(sub)
                sub_tripdistance_sum = round(collect(reduce(+, sub[:, :trip_distance])))
                sub_valid =
                    round(collect(reduce(&, map(d -> d > 1.0, sub[:, :trip_distance]))))
                #sub_hour_sum = collect(
                #    reduce(
                #        +,
                #        map(
                #            t -> hour(DateTime(t, "yyyy-mm-dd HH:MM:SS")),
                #            tripdata[:, :pickup_datetime],
                #        ),
                #    )
                #)

                # Assert
                @test sub_nrow == 15109122
                @test sub_tripdistance_sum == 5.3284506e7
                @test sub_valid
                #@test sub_hour_sum == 835932637
            end
        end
    end
end

@testset "Groupby and GDF operations" begin
    run_with_job("Groupby basic for initial functionality") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_basic_tests(bucket)

        for i = 1:2
            for path in [
                "s3://$(bucket)/iris_large.csv",
                "s3://$(bucket)/iris_large.parquet",
                "s3://$(bucket)/iris_large.arrow",
            ]
                df = read_file(path)

                # Assert that exception gets thrown for parameters that aren't supported
                @test_throws ErrorException groupby(df, :species; sort = false)

                # Groupby all columns
		gdf = groupby(df, :)
                @test length(gdf) == 882

                # Groupby first four columns
                gdf = groupby(df, Cols(Between(1, 4)))
                @test length(gdf) == 147

                # Groupby multiple columns selected using regex
                gdf = groupby(df, r".*width")
                @test length(gdf) == 97

                gdf_select_save_path = "s3://$(bucket)/test-tmp_gdf_select_$(split(path, "/")[end])"
                gdf_transform_save_path = "s3://$(bucket)/test-tmp_gdf_transform_$(split(path, "/")[end])"
                gdf_subset_save_path = "s3://$(bucket)/test-tmp_gdf_subset_$(split(path, "/")[end])"

                gdf = groupby(df, :species)

                # Assert exception gets thrown for parameters that aren't supported
                @test_throws ArgumentError select(
                    gdf,
                    :,
                    [:petal_length] => (pl) -> pl .- mean(pl);
                    ungroup = false,
                )
                @test_throws ArgumentError select(
                    gdf,
                    :,
                    [:petal_length] => (pl) -> pl .- mean(pl);
                    copycols = false,
                )
                @test_throws ArgumentError transform(
                    gdf,
                    :species => x -> "iris-" .* x;
                    ungroup = false,
                )
                @test_throws ArgumentError transform(
                    gdf,
                    :species => x -> "iris-" .* x;
                    copycols = false,
                )
                @test_throws ArgumentError subset(
                    gdf,
                    :petal_length => pl -> pl .>= mean(pl);
                    ungroup = false,
                )
                @test_throws ArgumentError subset(
                    gdf,
                    :petal_length => pl -> pl .>= mean(pl);
                    copycols = false,
                )

                # Groupby species and perform select, transform, subset
                if i == 1
                    gdf_select = select(gdf, :, [:petal_length] => (pl) -> pl .- mean(pl))
                    gdf_transform = transform(gdf, :species => x -> "iris-" .* x)
                    gdf_subset = subset(gdf, :petal_length => pl -> pl .>= mean(pl))
                    write_file(gdf_select_save_path, gdf_select)
                    write_file(gdf_transform_save_path, gdf_transform)
                    write_file(gdf_subset_save_path, gdf_subset)
                else
                    gdf_select = read_file(gdf_select_save_path)
                    gdf_transform = read_file(gdf_transform_save_path)
                    gdf_subset = read_file(gdf_subset_save_path)
                end

                # Collect results
                gdf_select_size = size(gdf_select)
                gdf_transform_size = size(gdf_transform)
                gdf_subset_nrow = nrow(gdf_subset)
                gdf_select_plf_subtract = round(
                    collect(reduce(-, gdf_select[:, :petal_length_function])),
                    digits = 2,
                )
                gdf_select_filter_length = nrow(
                    collect(
                        gdf_select[:, [:petal_length]][
                            map(l -> l .== 1.3, gdf_select[:, :petal_length]),
                            :,
                        ],
                    ),
                )
                gdf_transform_length =
                    length(groupby(gdf_transform, [:species, :species_function]))
                gdf_subset_collected = sort(collect(gdf_subset))
                gdf_subset_row5 = collect(gdf_subset_collected[5, :])
                gdf_subset_row333 = collect(gdf_subset_collected[333, :])
                gdf_subset_row474 = collect(gdf_subset_collected[474, :])
                # gdf_keepkeys_false_names = names(combine(gdf, nrow, keepkeys = false))
                gdf_keepkeys_true_names = Set(names(combine(gdf, nrow, keepkeys = true)))
                petal_length_mean =
                    sort(collect(combine(gdf, :petal_length => mean)), :petal_length_mean)[
                        :,
                        :petal_length_mean,
                    ]
                temp = combine(gdf, :petal_length => mean, renamecols = false)
                temp_names = Set(names(temp))
                temp_petal_length = sort(collect(temp)[:, :petal_length])

                # Assert
                @test gdf_select_size == (900, 6)
                @test gdf_transform_size == (900, 6)
                @test gdf_subset_nrow == 474
                @test gdf_select_plf_subtract == -0.13
                @test gdf_select_filter_length == 42
                @test gdf_transform_length == 18
                @test gdf_subset_row5 == [4.6, 3.1, 1.5, 0.2, "species4"]
                @test gdf_subset_row333 == [6.7, 2.5, 5.8, 1.8, "species_18"]
                @test gdf_subset_row474 == [7.9, 3.8, 6.4, 2.0, "virginica"]

                # Combine
                # @test gdf_keepkeys_false_names == ["nrow"]
                @test gdf_keepkeys_true_names == Set(["nrow", "species"])
                @test petal_length_mean == [
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                ]
                @test temp_names == Set(["petal_length", "species"])
                @test temp_petal_length == [
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    1.464,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                    5.552,
                ]
            end
        end
    end

    run_with_job("Groupby stress for initial functionality") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_stress_tests(bucket)

        for i = 1:2
            for path in [
                "s3://$(bucket)/tripdata_large_csv.csv",
                "s3://$(bucket)/tripdata_large_parquet.parquet",
                "s3://$(bucket)/tripdata_large_arrow.arrow",
            ]
                df = read_file(path)
                global n_repeats
                @test nrow(df) == 61577490 * n_repeats

                gdf_subset_save_path = get_save_path(bucket, "gdf_subset", path)
                if i == 1
                    gdf = groupby(df, [:passenger_count, :vendor_id])
                    gdf_subset = subset(gdf, :trip_distance => d -> d .>= round(mean(d)))
                    @test length(gdf) == 21
                else
                    gdf_subset = read_file(gdf_collect_save_path)
                end

                gdf_nrow = sort(collect(combine(gdf, nrow)))
                gdf_subset_nrow = nrow(gdf_subset)
                gdf_tripdistance_sum =
                    round(collect(reduce(+, gdf_subset[:, :trip_distance])))

                @test gdf_nrow == [
                    1,
                    1,
                    1,
                    2,
                    3,
                    3,
                    4,
                    25,
                    142,
                    15371,
                    601461,
                    609517,
                    1116194,
                    1258507,
                    1424146,
                    1453823,
                    4133487,
                    4412096,
                    4510571,
                    18330823,
                    23711312,
                ]
                @test gdf_subset_nrow == 16964379
                @test gdf_tripdistance_sum == 1.09448617e8
            end

        end
    end
end

@testset "Filter and groupby on empty dataframe" begin
    run_with_job("Filtering stress for initial functionality") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_stress_tests(bucket)

        for i = 1:2
            for path in ["s3://$(bucket)/empty_df.csv", "s3://$(bucket)/empty_df.arrow"]
            end
            df = read_file(path)

            @test size(df) == (0, 0)

            filtered_save_path = get_save_path(bucket, "filtered", path)
            if i == 1
                filtered = filter(row -> row.x == 0, df)
                write_file(filtered_save_path, filtered)
            else
                filtered = read_file(filtered_save_path)
            end

            filtered_size = size(filtered)
            filtered_grouped_nrow = size(combine(groupby(filtered, All()), nrow))
            grouped_length = length(groupby(df, All()))

            @test filtered_size == (0, 0)
            @test filtered_grouped_nrow == (0, 1)

            @test grouped_length == 0
        end
    end
end
