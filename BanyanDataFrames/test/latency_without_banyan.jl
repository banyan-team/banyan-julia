using DataFrames, CSV, Statistics

function main()
    println("download")
    @time f = Downloads.download("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv")
    println("CSV.read")
    @time iris_df = CSV.read(f, DataFrame)
    println("filter")
    @time iris_sub = filter(row -> row.petal_length < 6.0, iris_df)
    println("groupby")
    @time gdf = groupby(iris_sub, :species)
    println("combine")
    @time res = combine(gdf, :petal_length => mean)
end

main()

# 29.913117 seconds (13.37 M allocations: 706.287 MiB, 0.83% gc time, 98.25% compilation time)