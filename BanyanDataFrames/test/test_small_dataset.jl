@testset "Basic usage of BanyanDataFrames on a small dataset" begin
    run_with_job("Filtering small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        # Filter
        iris_filtered =
            filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), iris)
        @test nrow(iris_filtered) == 51
        unique_species = collect(unique(iris[!, :species]))
        @test unique_species == ["setosa", "versicolor", "virginica"]
        @test sum(nonunique(iris)) == 3
        @test nrow(dropmissing(iris)) == 150
    end

    run_with_job("Sorting small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        # Sort
        new = sort(iris, :sepal_width)
        sorted_iris = collect(new)
        @test first(sorted_iris)[:sepal_width] == 2.0
        @test first(sorted_iris)[:petal_length] == 3.5
        @test last(sorted_iris)[:sepal_width] == 4.4
        @test last(sorted_iris)[:petal_length] == 1.5
    end

    run_with_job("Join and group-by-aggregate small dataset") do job
        # Join and groupby aggregration
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")
        species_info = DataFrame(
            species = ["setosa", "versicolor", "virginica"],
            region = ["Arctic", "Eastern Canada", "Southeastern United States"],
        )
        new = innerjoin(iris, species_info, on = :species)
        @test ncol(new) == 6

        gdf_species = groupby(new, :species)
        gdf_region = groupby(new, :region)

        g_cols = collect(groupcols(gdf_region))
        v_cols = collect(valuecols(gdf_region))

        @test g_cols = [:region]
        @test v_cols == [:sepal_length, sepal_width, :petal_length, :petal_width, :species]

        lengths_species = collect(combine(gdf_species, :petal_length => mean))
        counts_species = collect(combine(gdf_region, nrow))
        length_region = collect(combine(gdf_region, :petal_length => mean))
        counts_region = collect(combine(gdf_region, nrow))

        @test first(lengths_species) == first(lengths_region)
        @test first(counts_species) == first(counts_region)
    end

    run_with_job("Group and map over small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        # Select
        iris_sepal_area =
            select(iris, 1, 2, [:sepal_length, :sepal_width] => (a, b) -> a .* b)
        first_row = first(collect(iris_sepal_area))
        last_row = last(collect(iris_sepal_area))
        @test first_row[:sepal_length_sepal_width_function] == 10.0
        @test last_row[:sepal_length_sepal_width_function] == 30.02

        # Transform
        iris_new = transform(gdf, :species => x -> "iris-" .* x, keepkeys = false)
        species_names = collect(unique(iris_new[!, "species_function"]))
        @test species_names == ["iris-setosa", "iris-versicolor", "iris-virginica"]

        # Split-Apply-Combine
        gdf = groupby(iris, :species)
        @test length(gdf) == 3
        iris_mins = collect(combine(gdf, :petal_length => minimum))
        @test iris_mins[!, :petal_length_minimum] == [1.0, 3.0, 4.5]

        # Subset
        long_petal_iris = combine(groupby(subset(iris, :petal_length => pl -> pl .>= 5.0), :species), nrow)
        @test collect(long_petal_iris)[!, :nrow] == [2, 44]
        @test collect(long_petal_iris)[!, :species] == ["versicolor", "virginica"]
    end
end

@testset "Complex usage of BanyanDataFrames on small dataset" begin
    run_with_job("Multiple operation")
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        # Compute the min-max normalized petal_length (within each species)
        # for each flower and sort by this value.

        # Method 1
        result_1 = sort(
	    transform(gdf, :petal_length => x -> (x .- minimum(x)) ./ (maximum(x) - minimum(x))),
            :petal_length_function
	)
        rename(result_1, :petal_length_function => :petal_length_normalized)
        result_1 = collect(result_1)
        
        # Method 2
        gdf = groupby(iris, :species)
        min_max = combine(gdf, :petal_length => minimum, :petal_length => maximum)
        iris_new = innerjoin(iris, min_max, on=:species)
        result_2 = sort(
	    select(iris_new, 1, 2, 3, 4, 5, [:petal_length, :petal_length_minimum, :petal_length_maximum] => (pl, min, max) ->(pl .- min) ./ (max .- min)),
	    :petal_length_petal_length_minimum_petal_length_maximum_function
	)
        rename(result_2, :petal_length_petal_length_minimum_petal_length_maximum_function => :petal_length_normalized)
        result_2 = collect(result_2)

        @test isapprox(getindex(result_1, 7, 6), 0.14287, atol=1e-4)
        @test isapprox(getindex(result_2, 7, 6), 0.14287, atol=1e-4)
        @test first(result_1)[:species] == first(result_2)[:species] == "setosa"
        @test last(result_1)[:species] == last(result_2)[:species] == "virginica"
        @test last(result_1)[:petal_length_normalized] == last(result_1)[:petal_length_normalized] == 1.0
    end
end

@testset "Complex usage of BanyanDataFrames on medium dataset" begin
    run_with_job("")
        # Upload data to S3
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        tripdata = read_csv("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv")
        write_csv(tripdata, get_s3fs_path(joinpath(bucket, "tripdata.csv")))

        # Read data and get properties
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")
        tripdata = dropmissing(tripdata)
        @test nrow(tripdata) == 10906858
        @test ncol(tripdata) == 19
        @test size(tripdata) == (10906858, 19)
        @test collect(names(tripdata))[1] == "VendorID"

        # Get all trips with distance longer than 1.0, group by passenger count,
        # and get the average trip distance
        distances = collect(
	    combine(
                groupby(
		    filter(
		        row -> row.trip_distance > 1.0, tripdata
		    ),
		    :passenger_count
		),
		:trip_distance => mean
            )
	)
        isapprox(first(distances)[:trip_distance_mean], 8.1954, atol=1e-3)
        isapprox(last(distances)[:trip_distance_mean], 8.2757, atol=1e-3)
        
        # 
    end
end
