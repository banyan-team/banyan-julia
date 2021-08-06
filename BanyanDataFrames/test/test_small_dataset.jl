@testset "Basic usage of BanyanDataFrames on a small dataset" begin
    run_with_job("Filtering") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://{bucket}/iris.csv")

        # Filter
        iris_filtered = filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), iris)
        num_rows = collect(nrows(iris_filtered))
 	@test num_rows == 51
        unique_species = collect(unique(iris[!, :species]))
        @test unique_species == ["setosa", "versicolor", "virginica"]
        num_nonunique = collect(sum(nonunique(iris)))
        @test num_nonunique == 3
    end

    run_with_job("Sorting")
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://{bucket}/iris.csv")
        
        # Sort
	new = sort(iris, :sepal_width)
	@test collect(first(new))[:sepal_width] == 2.0
        @test collect(first(new))[:petal_length] == 3.5
        @test collect(last(new))[:sepal_width] == 4.4
        @test collect(last(new))[:petal_length] == 1.5
    end

    run_with_job("Join and Groupby Aggregration")
        # Join and groupby aggregration
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://{bucket}/iris.csv")
        species_info = DataFrame(species=["setosa", "versicolor", "virginica"], region=["Arctic", "Eastern Canada", "Southeastern United States"])
        new = innerjoin(iris, species_info, on=:species)
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
end

