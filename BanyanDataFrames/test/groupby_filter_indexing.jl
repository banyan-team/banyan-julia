@testset "Filtering basic for initial functionality with $scheduling_config" for scheduling_config in
                                                                                 [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_basic_data()

        bucket = get_cluster_s3_bucket_name()

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
                    invalidate_source(sub2)
                    invalidate_sample(sub2)
                    write_file(sub2_save_path, sub2)
                else
                    sub = read_file(sub_save_path)
                    sub2 = read_file(sub2_save_path)
                end

                @show i
                @show sample(sub2)

                # Collect results
                sub_nrow = nrow(sub)
                sub2_nrow = nrow(sub2)
                @show sample(sub2)
                sepal_length_sub_sum = round(collect(reduce(+, sub[:, :sepal_length])))
                sepal_length_sub2_sum = round(collect((reduce(+, sub2[:, :sepal_length]))))
                @show sample(sub2)
                @show sample(df)
                @show collect(sub2)
                @show collect(sub2)
                @show sample(sub2)
                @show collect(sub2[:, [:species]])
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
                        filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), df),
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
                    sub4 = filter([:petal_length, :petal_width] => (l, w) -> l * w > 12.0, df)
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
end

@testset "Filtering stress for initial functionality with $scheduling_config" for scheduling_config in
                                                                                  [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_basic_data()

        bucket = get_cluster_s3_bucket_name()

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
                    sub =
                        filter(row -> (row.passenger_count != 1 && row.trip_distance > 1.0), df)
                    write_file(sub_save_path, sub)
                else
                    sub = read_file(sub_save_path)
                end

                # Collect results
                sub_nrow = nrow(sub)
                sub_tripdistance_sum = round(collect(reduce(+, sub[:, :trip_distance])))
                sub_valid = round(collect(reduce(&, map(d -> d > 1.0, sub[:, :trip_distance]))))
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

@testset "Groupby basic for initial functionality with $scheduling_config" for scheduling_config in
                                                                               [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_basic_data()

        bucket = get_cluster_s3_bucket_name()

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
                @test gdf_subset_nrow == 474
                gdf_subset_collected = sort(collect(gdf_subset))
                gdf_subset_row474 = collect(gdf_subset_collected[474, :])
                gdf_select_plf_square_add = round(
                    collect(reduce(+, map(l -> l * l, gdf_select[:, :petal_length_function]))),
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
                petal_length_mean = map(m -> round(m, digits = 2), petal_length_mean)
                temp = combine(gdf, :petal_length => mean, renamecols = false)
                temp_names = Set(names(temp))
                temp_petal_length = sort(collect(temp)[:, :petal_length])
                temp_petal_length = map(l -> round(l, digits = 2), temp_petal_length)

                # Assert
                @test gdf_select_size == (900, 6)
                @test gdf_transform_size == (900, 6)
                @test gdf_subset_nrow == 474
                @test gdf_select_plf_square_add == 163.32
                @test gdf_select_filter_length == 42
                @test gdf_transform_length == 18
                @test gdf_subset_row5 == [4.6, 3.1, 1.5, 0.2, "species_4"]
                @test gdf_subset_row333 == [6.7, 2.5, 5.8, 1.8, "species_18"]
                @test gdf_subset_row474 == [7.9, 3.8, 6.4, 2.0, "virginica"]

                # Combine
                # @test gdf_keepkeys_false_names == ["nrow"]
                @test gdf_keepkeys_true_names == Set(["nrow", "species"])
                @test petal_length_mean == [
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                ]
                @test temp_names == Set(["petal_length", "species"])
                @test temp_petal_length == [
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    1.46,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    4.26,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                    5.55,
                ]
            end
        end
    end
end


@testset "Groupby stress for initial functionality with $scheduling_config" for scheduling_config in
                                                                                [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_stress_data()

        bucket = get_cluster_s3_bucket_name()

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
                gdf_tripdistance_sum = round(collect(reduce(+, gdf_subset[:, :trip_distance])))

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

# Filter from dataset to one row or empty df
@testset "Filter and groupby with $scheduling_config for simple edge cases for $filetype" for scheduling_config in [
    "default scheduling",
    "size exaggurated",
], filetype in [
    "csv",
    "parquet",
    "arrow",
    "directory"
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_empty_data()

        bucket = get_cluster_s3_bucket_name()

        path = ""
        if filetype == "directory"
            path = "s3://$(bucket)/iris_large_dir.csv"
        else
            path = "s3://$(bucket)/iris_large.$(filetype)"
        end

        for i = 1:2
            # Read empty df
            df = read_file(path)

            # Filter which results in an empty df and in a df with a single entry
            filtered_empty_save_path = get_save_path(bucket, "filtered_empty", path)
            filtered_single_save_path = get_save_path(bucket, "filtered_single", path)
            if i == 1
                filtered_empty = filter(row -> row.petal_length > 10, df)
                filtered_single = filter(row -> row.petal_length == 1.4 && row.sepal_length == 4.9 && row.species == "setosa", df)
            else
                filtered_empty = read_file(filtered_empty_save_path)
                filtered_single = read_file(filtered_single_save_path)
            end

            has_schema = i != 2 || filetype == "arrow"
            has_num_cols = i != 2 || filetype != "parquet"

            # Test sizes
            filtered_empty_size = size(filtered_empty)
            filtered_single_size = size(filtered_single)
            @test filtered_empty_size == (has_num_cols ? (0, 5) : (0, 0))
            @test filtered_single_size == (1, 5)

            # Test downloading single row
            filtered_single_sepal_width = collect(filtered_single[:, :sepal_width])
            filtered_single_petal_width = collect(filtered_single[:, :petal_width])
            @test filtered_single_sepal_width == [3.0]
            @test filtered_single_petal_width == [0.2]

            # Only empty Arrow datasets preserve the schema and can be read
            # back in and used in a groupby-subset that references a column
            # from the original schema. Even if the computation were replicated
            # so that no grouping splitting has to be done, we still have to do
            # a groupby-subset on an empty DataFrame with no schema and
            # DataFrames.jl doesn't support that.
            if has_schema
                # Groupby all columns and subset, resulting in empty df
                filtered_empty_sub = subset(groupby(filtered_empty, :species), :petal_length => pl -> pl .>= mean(pl))
                filtered_empty_sub_size = size(filtered_empty_sub)
                @test filtered_empty_sub_size == (0, 5)
            end

            # Test size after filtering single-row dataset
            filtered_single_sub = subset(groupby(filtered_single, :species), :petal_length => pl -> pl .>= mean(pl))
            filtered_single_sub_size = size(filtered_single_sub)
            @test filtered_single_sub_size == (1, 5)

            # If this is round 1, write it out so that it can be read in round
            # 2
            if i == 1
                write_file(filtered_empty_save_path, filtered_empty)
                write_file(filtered_single_save_path, filtered_single)
            end
        end
    end
end

# Complex multi-step filtering to empty dataframes or single-row dataframe
@testset "Groupby and $filter_type with $scheduling_config for complex edge cases for $filetype" for scheduling_config in [
    "default scheduling",
    # We just want to test exaggurating the size so that the scheduler tries
    # to apply batched parallelism. It's okay that we aren't testing simple
    # parallelism because the same splitting functions are being called and
    # cast functions may be called when we do default scheduling so we're still
    # sort of testing that. Basically we just want to test filtering from a
    # really large dataset to an empty or single-row result.
    "size exaggurated",
], filetype in [
    "csv",
    "parquet",
    "arrow",
    "directory"
], filter_type in [
    "filter",
    "subset"
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_basic_data()

        bucket = get_cluster_s3_bucket_name()

        path = ""
        if filetype == "directory"
            path = "s3://$(bucket)/iris_large_dir.csv"
        else
            path = "s3://$(bucket)/iris_large.$(filetype)"
        end

        # Read empty df
        df = read_file(path)

        # Filter/subset to single row
        # Filter/subset to empty
        # Call collect on single row
        # Call collect on empty
        # Filter/subset to empty
        # Call collect on empty
        if filter_type == "filter"
            filt1 = filter(row -> row.petal_length == 1.4 && row.sepal_length == 4.9 && row.species == "setosa", df)
            filt2 = filter([:petal_length, :sepal_length] => (pl, sl) -> pl * sl > 100.0, filt1)
        elseif filter_type == "subset"
            filt1 = subset(groupby(df, :species), [:petal_length, :sepal_length, :species] => (pl, sl, s) -> ((pl .< mean(pl)) .& (sl .== 4.9) .& (s .== "setosa")))
            filt2 = subset(groupby(filt1, :species), :sepal_width => sw -> sw .> 100 * mean(sw))
        end
        df1 = collect(filt1)
        df2 = collect(filt2)
        if filter_type == "filter"
            filt3 = filter(row -> row.petal_width == 100, filt2)
        elseif filter_type == "subset"
            filt3 = subset(groupby(filt1, :petal_width), :petal_width => pw -> pw .== 100)
        end
        df3 = collect(filt3)

        @test size(df1) == (1, 5)
        @test names(df1) == ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
        @test collect(df1[1, :]) == [4.9, 3.0, 1.4, 0.2, "setosa"]

        @test size(df2) == (0, 5)
        @test names(df2) == ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

        @test size(df3) == (0, 5)
        @test names(df3) == ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

        
        # Filter/subset to single row
        # Filter/subset to empty
        # Filter/subset to empty
        # Call collect
        if filter_type == "filter"
            filt01 = filter(row -> row.sepal_length == row.sepal_width + 1.9 && row.species == "species_10", df)
            filt02 = filter(
                row -> row.petal_length == row.sepal_length * 2,
                filter(row -> row.species == "setosa", filt01)
            )
        elseif filter_type == "subset"
            filt01 = subset(groupby(df, :species), [:petal_length, :sepal_length, :species] => (pl, sl, s) -> ((pl .< mean(pl)) .& (sl .== 4.9) .& (s .== "species_10")))
            filt02 = subset(
                groupby(
                    subset(
                        groupby(filt01, :petal_width),
                        :petal_width => pw -> pw .== 100
                    ),
                    :species
                ),
                :petal_width => pw -> pw .== 100
            )
        end
        df01 = collect(filt01)
        df02 = collect(filt02)

        @test size(df01) == (1, 5)
        @test collect(df01[1, :]) == [4.9, 3.0, 1.4, 0.2, "species_10"]

        @test size(df02) == (0, 5)
        @test names(df02) == ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

    end
end

# Reading an empty dataset
@testset "Filter and groupby with $scheduling_config for empty $filetype with $headertype" for scheduling_config in [
    "default scheduling",
    # It doesn't make sense to force parallelism or batching for empty data
    # because for the case of an empty dataset, we will actually just use
    # replication because the data size is zero.
    # "parallelism encouraged",
    # "parallelism and batches encouraged",
], (filetype, headertype) in [
    ("csv", "header"),
    ("csv", "no header"),
    ("arrow", "header"),
    ("arrow", "no header"),
    ("directory", "no header"),
]
    use_job_for_testing(scheduling_config_name = scheduling_config) do
        use_empty_data()

        bucket = get_cluster_s3_bucket_name()

        path = ""
        if filetype == "directory"
            # Create empty directory
            path = "s3://$(bucket)/empty_dir.parquet"
            if !ispath(S3Path(path * "/", config = Banyan.get_aws_config()))
                mkpath(S3Path(path * "/", config = Banyan.get_aws_config()))
            end
            headertype = "no header"
        else
            if headertype == "header"
                path = "s3://$(bucket)/empty_df2.$(filetype)"
            elseif headertype == "no header"
                path = "s3://$(bucket)/empty_df.$(filetype)"
            end
        end
        

        for i = 1:2
            # Read empty df
            df = read_file(path)

            if headertype == "header"
                @test size(df) == (0, 2)
            elseif headertype == "no header"
                @test size(df) == (0, 0)
            end

            # Filter empty df, which should result in empty df
            filtered_save_path = get_save_path(bucket, "filtered", path)
            if i == 1
                if headertype == "header"
                    filtered = filter([:x, :y] => (x, y) -> x == y, df)
                elseif headertype == "no header"
                    filtered = filter(row -> row.x == 0, df)
                end
                write_file(filtered_save_path, filtered)
            else
                filtered = read_file(filtered_save_path)
            end

            # Groupby all columns and aggregrate to count number of rows
            filtered_size = size(filtered)
            filtered_grouped = groupby(filtered, All())
            filtered_grouped_nrow = size(combine(filtered_grouped, nrow))
            filtered_grouped_length = length(groupby(df, All()))

            if headertype == "header"
                @test filtered_size == (0, 2)
                @test filtered_grouped_nrow == (0, 3)
                @test filtered_grouped_length == 0
            elseif headertype == "no header"
                @test filtered_size == (0, 0)
                @test filtered_grouped_nrow == (0, 1)
                @test filtered_grouped_length == 0
            end
        end
    end
end

@testset "NYC Taxi Stress Test" begin
    using Statistics
    use_job_for_testing(scheduling_config_name = "default scheduling", sample_rate=128) do
        s3_bucket_name = get_cluster_s3_bucket_name()
        df = read_csv(
	    "s3://$s3_bucket_name/nyc_tripdata.csv",
	    sample_invalid=true
	)
        # @show sample(df)
        println("Finished reading df")

        # Filter all trips with distance longer than 1.0. Group by passenger count
        # and get the average trip distance for each group.
        long_trips = filter(
            row -> row.trip_distance > 1.0,
            df
        )
        println("Finished filtering to long_trips")
        # @show sample(long_trips)

        gdf = groupby(long_trips, :passenger_count)
        println("Finished groupby by passenger count to gdf")
        trip_means = combine(gdf, :trip_distance => mean)
        println("Finished combining by mean to trip_means")

        trip_means = collect(trip_means)
        println("Finished collecting to trip_means")

    end
end
