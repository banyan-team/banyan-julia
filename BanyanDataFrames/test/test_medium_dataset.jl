@testset "Complex usage of BanyanDataFrames on medium dataset" begin
    run_with_job("Filter groupby") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        tripdata = read_csv("s3://{bucket}/tripdata.csv")

        # Read data and get properties
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")
        tripdata = dropmissing(tripdata)
        @test nrow(tripdata) == 10906858
        @test ncol(tripdata) == 19
        @test size(tripdata) == (10906858, 19)
        @test collect(names(tripdata))[1] == "VendorID"

        # Get all trips with distance longer than 1.0, group by passenger count,
        # and get the average trip distance for each group
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
    end

    run_with_job("Groupby/sort DateTime") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        tripdata = read_csv("s3://{bucket}/tripdata.csv")

        # Compute the hour of the day which has the greatest average trip distance.
        setindex!(tripdata, :start_time, DateTime.(tripdata[:, :tpep_pickup_datetime], "yyyy-mm-dd HH:MM:SS"))
        setindex!(tripdata, :start_hour, hour.(tripdata[:, :start_time]))
        tripdata_grouped = groupby(tripdata, :start_hour)
        means = combine(tripdata_grouped, :trip_distance => mean)
        @test collect(sort(means, :trip_distance_mean)) == 0
    end
end
