function upload_tripdata_large_to_s3(bucket_name)
    # Construct directory of 12 csv files for NYC taxi records from 2012 (total 30 GB) duplicated ncopies times
    ncopies = 3
    for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        for ncopy in 1:ncopies
            verify_file_in_s3(
                bucket_name,
                "tripdata_large/tripdata_$(month)_copy$(ncopy).csv",
                "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-$(month).csv",
            )
        end
    end
end

@testset "Stress test of BanyanDataFrames on a large dataset" begin
    run_with_job("Stress test on multi-file dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_tripdata_large_to_s3(bucket)
        tripdata_large = read_csv("s3://$(bucket)/tripdata_large")

        # Select trips with more than 1 passenger and group by the passenger count.
        # 1 - Compute the fraction of trips for each group that have a trip distance longer
        # than the average trip distance for the group.
        # 2 - 

        tripdata_large = dropmissing(tripdata_large)
        tripdata_large_filtered = filter(
	    row -> (
	        row.passenger_count > 1
	    ),
	    tripdata_large
	)
        res = transform(
            groupby(tripdata_large_filtered, :passenger_count),
	    :trip_distance => d -> d .> mean(d)
	)
        gdf = groupby(res, :passenger_count)
        res = combine(
            groupby(res, :passenger_count),
	    nrow,
	    :trip_distance_function => sum
	)
        res[:, :fraction] = map((n, s) -> s / n, res[:, :nrow], res[:, :trip_distance_function_sum])

        res = collect(res)
        # @test res[:, :fraction] == [??]
        
    end
end
