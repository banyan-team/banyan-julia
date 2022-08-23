using Statistics

function test_csv_from_internet_latency()
    use_session_for_testing(scheduling_config_name = "default scheduling", sample_rate=256) do
        for i in 1:1
            println(i == 1 ? "Cold start" : "Warm start")
            @time begin
                println("read_csv with source and sample invalid but shuffled")
                @time begin
                    iris_df = read_csv(
                        "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv",
                        shuffled=true,
                        metadata_invalid=true,
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
                # avg_pl = @time compute(combine(gdf, :petal_length => mean))
                println("Total time: after starting session")
            end
        end
    end
end

function test_csv_from_s3_latency()
    @time "start_session" begin
    s = start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = parse(Int64, get(ENV, "BANYAN_NWORKERS", "2")),
        # sample_rate = 4096,
        print_logs = true,
        url = "https://github.com/banyan-team/banyan-julia.git",
        branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory = "banyan-julia/BanyanDataFrames/test",
        dev_paths = [
            "banyan-julia/Banyan",
            "banyan-julia/BanyanArrays",
            "banyan-julia/BanyanDataFrames"
        ],
        # BANYAN_REUSE_RESOURCES should be 1 when the compute resources
        # for sessions being run can be reused; i.e., there is no
        # forced pulling, cloning, or installation going on. When it is
        # set to 1, we will reuse the same job for each session. When
        # set to 0, we will use a different job for each session but
        # each session will immediately release its resources so that
        # it can be used for the next session instead of giving up
        # TODO: Make it so that sessions that can't reuse existing jobs
        # will instead destroy jobs so that when it creates a new job
        # it can reuse the existing underlying resources.
        release_resources_after = get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? 20 : 0,
        force_pull = get(ENV, "BANYAN_FORCE_PULL", "0") == "1",
        force_sync = get(ENV, "BANYAN_FORCE_SYNC", "0") == "1",
        force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
        store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
        force_new_pf_dispatch_table = true,
        # estimate_available_memory=true
        # log_initialization=true
    )
    end

    try
        s3_bucket_name = get_cluster_s3_bucket_name()
        if !s3_exists(Banyan.global_aws_config(), s3_bucket_name, "nyc_tripdata_small.parquet")
            data_path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
            offloaded(s3_bucket_name, data_path) do s3_bucket_name, data_path
                temp_path = Downloads.download(data_path)
                cp(
                    Path(temp_path),
                    S3Path("s3://$s3_bucket_name/nyc_tripdata_small.parquet", config=Banyan.global_aws_config())
                )
            end
        end

        for invalid in [true]#, false]
            for i in 1:2
                @time (i == 1 ? "Cold start with invalid=$invalid" : "Warm start with invalid=$invalid") begin
                    println("read_csv with source and sample invalid but shuffled")
                    df = @time "read_parquet" begin
                        BanyanDataFrames.read_parquet(
                            "s3://$s3_bucket_name/nyc_tripdata_small.parquet",
                            shuffled=true,
                            metadata_invalid=invalid,
                            samples_invalid=invalid
                        )
                    end

                    # @show DataFrames.size(sample(df))

                    long_trips = @time "filter" filter(
                        row -> row.trip_distance > 2.0,
                        df
                    )

                    # @show DataFrames.size(sample(long_trips))

                    gdf = @time "groupby" groupby(long_trips, :passenger_count)

                    trip_means = @time "combine" combine(gdf, :trip_distance => mean)

                    trip_means = @time "compute" compute(trip_means)
                end
            end
        end
    catch
        @time "end_session" begin
        end_session(s)
        end
        rethrow()
    finally
        end_session(s)
    end
end