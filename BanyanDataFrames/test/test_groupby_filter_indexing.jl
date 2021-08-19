######################################
# SETUP AND CLEANUP HELPER FUNCTIONS #
######################################

function write_df_to_csv_to_s3(df, filename, filepath, bucket_name)
    CSV.write(filename, df)
    verify_file_in_s3(
        bucket_name,
        filename,
        filepath
    )
end

function write_df_to_parquet_to_s3(df, filename, filepath, bucket_name)
    Parquet.write_parquet(filename, df)
    verify_file_in_s3(
        bucket_name,
        filename,
        filepath
    )
end

function write_df_to_arrow_to_s3(df, filename, filepath, bucket_name)
    Arrow.write(filename, df)
    verify_file_in_s3(
        bucket_name,
        filename,
        filepath
    )
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
    for i in 4:18
        species_list = append!(species_list, Base.fill("species_$(i)", 50))
    end
    df[:, :species] = species_list
    write_df_to_csv_to_s3(df, "iris_large.csv", p"iris_large.csv", bucket_name)
    write_df_to_parquet_to_s3(df, "iris_large.parquet", p"iris_large.parquet", bucket_name)
    write_df_to_arrow_to_s3(df, "iris_large.arrow", p"iris_large.arrow", bucket_name)

    write_df_to_csv_to_s3(df_s, "iris_species_info.csv", p"iris_species_info.csv", bucket_name)
    write_df_to_parquet_to_s3(df_s, "iris_species_info.parquet", p"iris_species_info.parquet", bucket_name)
    write_df_to_arrow_to_s3(df_s, "iris_species_info.arrow", p"iris_species_info.arrow", bucket_name)
end

global n_repeats = 10

function setup_stress_tests(bucket_name)
    global n_repeats
    for month in ["01", "02", "03", "04"]
        for ncopy in 1:n_repeats
            download_path = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-$(month).csv"
            local_path = download(download_path)
            df = CSV.read(local_path, DataFrames.DataFrame)
            write_df_to_csv_to_s3(df, "tripdata_large_csv/tripdata_$(month)_copy$(ncopy).csv", "tripdata.csv")
            write_df_to_parquet_to_s3(df, "tripdata_large_parquet/tripdata_$(month)_copy$(ncopy).parquet", "tripdata.parquet")
            write_df_to_arrow_to_s3(df, "tripdata_large_arrow/tripdata_$(month)_copy$(ncopy).arrow", "tripdata.arrow")
        end
    end
end

function cleanup_tests(bucket_name)
    # Delete all temporary test files that are prepended with "test-tmp__"
    for obj in s3_list_objects(bucket_name, path_prefix="test-tmp_")
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
        BanyanDataFrames.write_csv(df, path)
    elseif endswith(path, ".arrow")
        BanyanDataFrames.write_csv(df, path)
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
    run_with_job("Filtering basic") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_basic_tests(bucket)

        for i in 1:2
            for path in [
	        "s3://$(bucket)/iris_large.csv",
	        "s3://$(bucket)/iris_large.parquet",
	        "s3://$(bucket)/iris_large.arrow"
	    ]
                df = read_file(path)

                # Assert that exception gets thrown for parameters that aren't supported
                @test_throws ArgumentError filter(row -> row.sepal_length > 5.0, df; view=true)

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
	        @test nrow(sub) == 36
	        @test round(collect(reduce(+, sub[:, :sepal_length]))) == 217
	        @test nrow(sub2) == 4
	        @test round((reduce(+, sub2[:, :sepal_length]))) == 26
	        @test Set(collect(sub2[:, :species])) == Set(["species_8", "species_18"])


	        # Filter based on multiple columns using DataFrameRow syntax
		sub3_save_path = get_save_path(bucket, "sub3", path)
		if i == 1
		    sub3 = filter(
	                row -> (row.species == "versicolor" || row.species == "species_17"),
		        filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), df)
	            )
		    write_file(sub3_save_path, sub3)
		else
		    sub3 = read_file(sub3_save_path)
		end
	        @test nrow(sub3) == 62
	        sub3 = sort(collect(sub3))
                @test collect(sub3[8, :]) == [5.6, 2.9, 3.6, 1.3, "versicolor"]
    	        @test collect(sub3[62, :]) == [5.7, 2.8, 4.1, 1.3, "species_17"]

	        # Filter based on multiple columns, using cols
		sub4_save_path = get_save_path(bucket, "sub4", path)
		if i == 1
	            sub4 = filter([:petal_length, :petal_width] => (l, w) -> l * w > 12.0, df)
		    write_file(sub4_save_path, sub4)
		else
		    sub4 = read_file(sub4_save_path)
		end
	        @test nrow(sub4) == 114
	        @test collect(reduce(max, sub4[:, :petal_length])) == 6.9
	        @test collect(reduce(&, map((l, w) -> l .* w .> 12, sub4[:, :petal_length], sub4[:, :petal_width]))) == true
	        sub4 = sort(collect(sub4))
	        @test collect(sub4[8, :]) == [6.2, 3.4, 5.4, 2.3, "species_15"]
	        @test collect(sub4[114, :]) == [7.9, 3.8, 6.4, 2.0, "virginica"]

	        # Filter based on multiple columns, using AsTable
		sub5_save_path = get_save_path(bucket, "sub5", path)
		if i == 1
	            sub5 = filter(AsTable(:) => iris -> (iris.petal_length % 2 == 0) && (iris.sepal_length > 6.0 || iris.petal_width < 0.5), df)
	            write_file(sub5_save_path, sub5)
		else
		    sub5 = read_file(sub5_save_path)
		end
		@test nrow(sub5) == 18
	        sub5 = sort(collect(sub5))
	        @test collect(sub5[1, :]) == [6.1, 2.8, 4.0, 1.3, "species_11"]
	        @test collect[(sub5[18, :]) == [7.2, 3.2, 6.0, 1.8, "virginica"]
	    end
        end
    end

    run_with_job("Filtering stress") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
	setup_basic_tests(bucket)

        for i in 1:2
	    for path in [
	        "s3://$(bucket)/tripdata_large_csv",
		"s3://$(bucket)/tripdata_large_parquet",
		"s3://$(bucket)/tripdata_large_arrow"
	    ]
	        df = read_file(path)
		@test nrow(df) == 61577490 * n_repeats

		sub_save_path = get_save_path(bucket, "sub", path)
		if i == 1
                    sub = filter(
		         row -> (
		             row.passenger_count != 1 && row.trip_distance > 1.0
		         ),
		         df
		     )
		     write_file(sub_save_path, sub)
		 else
		     sub = read_file(sub_save_path)
		 end

		 @test nrow(sub) == 15109122
		 @test round(reduce(+, sub[:, :trip_distance])) == 5.3284506e7
		 @test round(reduce(&, map(d -> d > 1.0, sub[:, :trip_distance])))
		 @test reduce(+, map(t -> hour(DateTime(t, "yyyy-mm-dd HH:MM:SS")), tripdata[:, :pickup_datetime])) == 835932637
	    end
	end
    end
end

@testset "Groupby and GDF operations" begin
    run_with_job("Groupby basic") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        setup_basic_tests(bucket)

        for i in 1:2
            for path in [
                "s3://$(bucket)/iris_large.csv",
                "s3://$(bucket)/iris_large.parquet",
                "s3://$(bucket)/iris_large.arrow"
            ]
                df = read_file(path)

                # Assert that exception gets thrown for parameters that aren't supported
                @test_throws ErrorException groupby(df, :species; sort=true)

                # Groupby all columns
                gdf = groupby(df, :)
                @test length(gdf) == 882

                # Groupby first four columns
                gdf = groupby(df, Cols(Between(1, 4)))
                @test length(gdf) == 147

                # Groupby multiple columns selected using regex
                gdf = groupby(df, r".*width")
                @test length(gdf) == 18

                gdf_select_save_path = "s3://$(bucket)/test-tmp_gdf_select_$(split(path, "/")[end])"
                gdf_transform_save_path = "s3://$(bucket)/test-tmp_gdf_transform_$(split(path, "/")[end])"
                gdf_subset_save_path = "s3://$(bucket)/test-tmp_gdf_subset_$(split(path, "/")[end])"

                gdf = groupby(df, :species)

                # Assert exception gets thrown for parameters that aren't supported
                @test_throws ArgumentError select(gdf, :, [:petal_length] => (pl) -> pl .- mean(pl); ungroup=true)
                @test_throws ArgumentError select(gdf, :, [:petal_length] => (pl) -> pl .- mean(pl); copycols=true)
                @test_throws ArgumentError transform(gdf, :species => x -> "iris-" .* x; ungroup=true)
                @test_throws ArgumentError transform(gdf, :species => x -> "iris-" .* x; copycols=true)
                @test_throws ArgumentError subset(gdf, :petal_length => pl -> pl .>= mean(pl); ungroup=true)
                @test_throws ArgumentError subset(gdf, :petal_length => pl -> pl .>= mean(pl); copycols=true)
	
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

                @test size(gdf_select) == (900, 6)
                @test size(gdf_transform) == (900, 6)
                @test nrow(gdf_subset) == 474
		@test round(collect(reduce(-, gdf_select[:, :petal_length_function])), digits=2) == -0.13
		@test length(collect(gdf_select[:, :petal_length][map(l -> l .== 1.3, gdf_select[:, :petal_length])])) == 42
		@test length(groupby(gdf_transform, [:species, :species_function])) == 18
		gdf_subset_collected = sort(collect(gdf_subset))
		@test collect(gdf_subset_collect[5, :]) == [4.6, 3.1, 1.5, 0.2]
		@test collect(gdf_subset_collect[33, :]) == [6.7, 2.5, 5.8, 1.8]
		@test collect(gdf_subset_collect[474, :]) == [7.9, 3.8, 6.4, 2.0, "virginica"]

                # Combine
		@test names(combine(gdf, nrow, keepkeys=false)) == ["nrow"]
		@test Set(names(combine(gdf, nrow, keepkeys=true))) == Set(["nrow", "species"])
		@test sort(collect(combine(gdf, :petal_length => mean)), :petal_length_mean)[:, :petal_length_mean] == [1.464, 1.464, 1.464, 1.464, 1.464, 1.464, 4.26, 4.26, 4.26, 4.26, 4.26, 4.26, 5.552, 5.552, 5.552, 5.552, 5.552, 5.552]
                temp = combine(gdf, :petal_length => mean, renamecols=false)
		@test Set(names(temp)) == Set(["petal_length", "species"])
		@test sort(collect(temp)[:petal_length]) == [1.464, 1.464, 1.464, 1.464, 1.464, 1.464, 4.26, 4.26, 4.26, 4.26, 4.26, 4.26, 5.552, 5.552, 5.552, 5.552, 5.552, 5.552]
            end
        end
    end

    run_with_job("Groupby stress") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
	setup_basic_tests(bucket)

	for i in 1:2
	    for path in [
	        "s3://$(bucket)/tripdata_large_csv",
		"s3://$(bucket)/tripdata_large_parquet",
		"s3://$(bucket)/tripdata_large_arrow"
	    ]
	        df = read_file(path)
		global n_repeats
		@test nrow(df) == 61577490 * n_repeats

                gdf_subset_save_path = get_save_path(bucket, "gdf_subset", path)
		if i == 1
		    gdf = groupby(df, [:passenger_count, :vendor_id])
		    gdf_subset = subset(gdf, :trip_distance => d -> d.>= round(mean(d)))
		    @test length(gdf) == 21
		else
		    gdf_subset = read_file(gdf_collect_save_path)
		end

		@test sort(collect(combine(gdf, nrow))) == [1, 1, 1, 2, 3, 3, 4, 25, 142, 15371, 601461, 609517, 1116194, 1258507, 1424146, 1453823, 4133487, 4412096, 4510571, 18330823, 23711312]
		@test nrow(gdf_subset) == 16964379
		@test round(collect(reduce(+, gdf_subset[:, :trip_distance]))) == 1.09448617e8
           end

	end
    end
end
