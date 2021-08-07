@testset "Read/Write to CSV" begin
    run_with_job("read/write small csv and compute properties") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://{bucket}/iris.csv")
        @test collect(nrow(iris)) == 150
        @test collect(ncol(iris)) == 5
        @test collect(names(iris)) == ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
        @test collect(propertynames(iris)) == [:sepal_length, :sepal_width, :petal_length, :petal_width, :species]
        @test collect(first(iris)) == DataFrameRow(DataFrame(sepal_length=5.1, sepal_width=3.5, petal_length=1.4, petal_width=0.2, species="setosa"), 1)
        @test collect(last(iris)) == DataFrameRow(DataFrame(sepal_length=5.9, sepal_width=3.0, petal_length=5.1, petal_width=1.8, species="virginica"), 1)

	# Append row and write to file
	push!(iris, (1.0, 2.0, 3.0, 4.0, "newspecies"))
	write_csv(iris, "s3://{bucket}/iris_new.csv")
        # Read file and verify
	iris_new = read_csv("s3://{bucket}/iris_new.csv")
	@test collect(nrow(iris_new)) == 151
	@test collect(ncol(iris_new)) == 5
	@test collect(last(iris)) == DataFrameRow(DataFrame(sepal_length=1.0, sepal_width=2.0, petal_length=3.0, petal_width=4.0, species="newspecies"), 1)
    end
    try
        s3_delete(get_cluster_s3_bucket_name(get_cluster().name), "iris_new.csv")
    catch
    end
end
