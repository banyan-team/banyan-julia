@testset "Complex usage of BanyanDataFrames on medium dataset" begin
    run_with_job("") do job
        # Upload data to S3
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        tripdata = read_csv("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv")
        write_csv(tripdata, get_s3fs_path(joinpath(bucket, "tripdata.csv")))

        # Read data and get properties
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")
        tripdata = dropmissing(tripdata)
        @test nrow(tripdata) == 10906858
        @test ncol(tripdata) == 19
        @test size(tripdata) == (10906858, 19)
        @test collect(names(tripdata))[1] == "VendorID"

        # Get all trips with distance longer than 1.0, group by passenger count,
        # and get the average trip distance
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

        #
    end
end
