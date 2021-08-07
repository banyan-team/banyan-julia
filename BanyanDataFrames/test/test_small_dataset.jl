@testset "Basic usage of BanyanDataFrames on a small dataset" begin
    run_with_job("Filtering small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        # Filter
        iris_filtered =
            filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), iris)
        num_rows = collect(nrows(iris_filtered))
        @test num_rows == 51
        unique_species = collect(unique(iris[!, :species]))
        @test unique_species == ["setosa", "versicolor", "virginica"]
        num_nonunique = collect(sum(nonunique(iris)))
        @test num_nonunique == 3
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
        ncols = collect(ncol(new))
        @test ncol == 6

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
        iris_mins = collect(combine(gdf, :petal_length => minimum))
        @test iris_mins[!, :petal_length_minimum] == [1.0, 3.0, 4.5]
    end
end

@testset "Complex usage of BanyanDataFrames on small dataset" begin
    run_with_job("Multiple operation")
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")

        #  
    end
end
