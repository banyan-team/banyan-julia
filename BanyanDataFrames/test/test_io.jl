function upload_iris_all_formats_to_s3(bucket_name)
    iris_download_path = "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
    iris_local_path = download(iris_download_path)
    df = CSV.read(iris_local_path, DataFrames.DataFrame)
    # CSV
    verify_file_in_s3(
        bucket_name,
        "iris.csv",
        iris_download_path,
    )
    #df = collect(read_csv("s3://$(bucket_name)/iris.csv"))
    # Parquet
    Parquet.write_parquet("iris.parquet", df)
    verify_file_in_s3(
        bucket_name,
	"iris.parquet",
	p"iris.parquet"
    )
    # Arrow
    Arrow.write("iris.arrow", df)
    verify_file_in_s3(
        bucket_name,
	"iris.arrow",
	p"iris.arrow"
    )
end

@testset "Read/Write to CSV, Parquet, and Arrow" begin
    run_with_job("read/write and compute properties") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_all_formats_to_s3(bucket)
        for filetype in ["csv", "parquet", "arrow"]
            read_func = if filetype == "csv" read_csv
                elseif filetype == "parquet" read_parquet
                elseif filetype == "arrow" read_arrow
                end
            iris = read_func("s3://$(bucket)/iris.$(filetype)")
            @test nrow(iris) == 150
            @test ncol(iris) == 5
            @test collect(names(iris)) ==
                ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
            @test collect(propertynames(iris)) ==
                [:sepal_length, :sepal_width, :petal_length, :petal_width, :species]
            iris_filter = iris[map(sl, sl -> sl == 5.1, iris[:, :sepal_length]), :]
            @test first(collect(iris_filter)) == DataFrameRow(
                DataFrame(
                    sepal_length = 5.1,
                    sepal_width = 3.5,
                    petal_length = 1.4,
                    petal_width = 0.2,
                    species = "setosa",
                ),
                1,
            )
            @test last(collect(iris_filter)) == DataFrameRow(
                DataFrame(
                    sepal_length = 5.1,
                    sepal_width = 2.5,
                    petal_length = 3.0,
                    petal_width = 1.1,
                    species = "versicolor",
                ),
                1,
            )

            # Write filtered dataset to file
            write_func = if filetype == "csv" write_csv
                elseif filetype == "parquet" write_parquet
                elseif filetype == "arrow" write_arrow
                end
            write_func(iris_filter, "s3://$(bucket)/iris_new.$(filetype)")
            # Read file and verify
            iris_new = read_func("s3://$(bucket)/iris_filter.$(filetype)")
            @test nrow(iris_new) == 9
            @test ncol(iris_new) == 5
            @test last(collect(iris_new)) == DataFrameRow(
                DataFrame(
                    sepal_length = 5.1,
                    sepal_width = 2.5,
                    petal_length = 3.0,
                    petal_width = 1.1,
                    species = "versicolor",
                ),
                1,
            )
        end
    end
    try
        s3_delete(get_cluster_s3_bucket_name(get_cluster().name), "iris_new.csv")
        s3_delete(get_cluster_s3_bucket_name(get_cluster().name), "iris_new.parquet")
        s3_delete(get_cluster_s3_bucket_name(get_cluster().name), "iris_new.arrow")
    catch
    end
end