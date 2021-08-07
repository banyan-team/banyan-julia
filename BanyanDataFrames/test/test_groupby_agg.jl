@testset "Basic usage of BanyanDataFrames" begin
    run_with_job("Simple group-by aggregation") do job
        # TODO: Use `get_cluster()` and `wget` to automatically load in iris.csv
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/iris.csv")
        gdf = groupby(iris, :species)
        lengths = collect(combine(gdf, :petal_length => mean))
        counts = collect(combine(gdf, nrow))

        @show lengths
        @show counts

        @test first(lengths)[:petal_length_mean] == 1.464
        @test first(counts)[:nrow] == 50
    end
end
