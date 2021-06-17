@testset "Simple usage of BanyanDataFrames" begin
    run_with_job("Grouping") do job
        # TODO: Use `get_cluster()` and `wget` to automatically load in iris.csv
        iris = read_csv("s3://banyan-cluster-data-pumpkincluster0-3e15290827c0c584/iris.csv")
        gdf = groupby(iris, :species)
        lengths = collect(combine(gdf, :petal_length => mean))
        counts = collect(combine(gdf, nrow))

        @test first(lengths)[:petal_length_mean] == 1.464
        @test first(counts)[:nrow] == 50
    end
end
