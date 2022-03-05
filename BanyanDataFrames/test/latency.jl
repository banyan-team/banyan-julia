function test_csv_from_internet_latency()
    use_session_for_testing(scheduling_config_name = "default scheduling", sample_rate=256) do
        for i in 1:2
            println(i == 1 ? "Cold start" : "Warm start")
            @time begin
                println("read_csv with source and sample invalid but shuffled")
                @time begin
                    iris_df = read_csv(
                        "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv",
                        shuffled=true,
                        source_invalid=true,
                        sample_invalid=true
                    )
                end

                println("filter")
                # Filters the rows based on whether the petal length is less than 6.0
                iris_sub = @time filter(row -> row.petal_length < 6.0, iris_df)

                println("groupby")
                # Groups the rows by the species type
                gdf = @time groupby(iris_sub, :species)

                println("combine")
                res = @time combine(gdf, :petal_length => mean)
                
                println("compute")
                # Computes average petal length for each group and stores in a new
                # DataFrame. Collects the result back to the client and materializes in
                # the variable `avg_pl`.
                avg_pl = @time compute(res)
                println("Total time:")
            end
        end
    end
end

function test_csv_from_s3_latency()
    use_session_for_testing(scheduling_config_name = "default scheduling", sample_rate=2048*4) do
        s3_bucket_name = get_cluster_s3_bucket_name()
        if !s3_exists(Banyan.get_aws_config(), s3_bucket_name, "nyc_tripdata_small.csv")
            data_path = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv"
            offloaded(s3_bucket_name, data_path) do s3_bucket_name, data_path
                temp_path = download(data_path)
                cp(
                    Path(temp_path),
                    S3Path("s3://$s3_bucket_name/nyc_tripdata_small.csv", config=Banyan.get_aws_config())
                )
            end
        end

        for invalid in [true, false]
            for i in 1:2
                println(i == 1 ? "Cold start with invalid=$invalid" : "Warm start with invalid=$invalid")
                @time begin
                    println("read_csv with source and sample invalid but shuffled")
                    df = @time begin
                        read_csv(
                            "s3://$s3_bucket_name/nyc_tripdata_small.csv",
                            shuffled=true,
                            source_invalid=invalid,
                            sample_invalid=invalid
                        )
                    end

                    println("filter")
                    long_trips = filter(
                        row -> row.trip_distance > 2.0,
                        df
                    )

                    println("groupby")
                    gdf = groupby(long_trips, :passenger_count)

                    println("combine")
                    trip_means = combine(gdf, :trip_distance => mean)

                    println("compute")
                    trip_means = @time compute(trip_means)

                    println("Total time:")
                end
            end
        end
    end
end