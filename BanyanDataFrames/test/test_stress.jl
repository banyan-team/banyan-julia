global n_repeats = 10


function upload_tripdata_large_to_s3(bucket_name)
    # # Construct directory of 12 csv files for NYC taxi records from 2012 (total 30 GB) duplicated ncopies times
    # ncopies = 3
    # for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    #     for ncopy in 1:ncopies
    #         verify_file_in_s3(
    #             bucket_name,
    #             "tripdata_large/tripdata_$(month)_copy$(ncopy).csv",
    #             "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2012-$(month).csv",
    #         )
    #     end
    # end

    # Construct directory with 4 csv files (~10 GB) replicated n_repeats times
    global n_repeats
    for month in ["01", "02", "03", "04"]
        for ncopy in 1:n_repeats
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
        global n_repeats

        # Test Coverage: dropmissing, filter, setindex!, getindex, rename, sort,
        #                length, groupcols, valuecols, groupby, transform, combine,
        #                unique
        # Missing: nonunique, innerjoin, select, subset, allowmissing,
        #          disallowmissing

        # Properties
        @test nrow(tripdata_large) == 61577490 * n_repeats
        @test ncol(tripdata_large) == 18
        @test size(tripdata_large) == (61577490 * n_repeats, 18)
        @test sort(names(tripdata_large)) == ["dropoff_datetime", "dropoff_latitude", "dropoff_longitude", "fare_amount", "mta_tax", "passenger_count", "payment_type", "pickup_datetime", "pickup_latitude", "pickup_longitude", "rate_code", "store_and_fwd_flag", "surcharge", "tip_amount", "tolls_amount", "total_amount", "trip_distance", "vendor_id"]
        @test sort(propertynames(tripdata_large)) == [:dropoff_datetime, :dropoff_latitude, :dropoff_longitude, :fare_amount, :mta_tax, :passenger_count, :payment_type, :pickup_datetime, :pickup_latitude, :pickup_longitude, :rate_code, :store_and_fwd_flag, :surcharge, :tip_amount, :tolls_amount, :total_amount, :trip_distance, :vendor_id]

        # Dropmissing
        tripdata_large = dropmissing(tripdata_large)
        @test nrow(tripdata_large) == 31210304 * n_repeats

        # Select trips with more than 1 passenger and group by the passenger count.
        # 1 - Compute the fraction of trips for each group that have a trip distance longer
        # than the average trip distance for the group.
        # 2 - Compute the min-max normalized 

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
        res = rename(res, :trip_distance_function => :is_distance_longer_than_mean)
        gdf = groupby(res, :passenger_count)
        res = rename(
	    combine(
                groupby(res, :passenger_count),
	        nrow,
	        :is_distance_longer_than_mean => sum
	    ),
	    :nrow => :n_trips,
	    :is_distance_longer_than_mean_sum => :total_trip_distance
	)
        res[:, :fraction] = map((n, s) -> round(s / n, digits=2), res[:, :n_trips], res[:, :total_trip_distance])
        res = sort(res, [:fraction, :n_trips])

        res_collected = collect(res[:, [:n_trips, :total_trip_distance, :fraction]])

	@test nrow(tripdata_large_filtered) == 6242979 * n_repeats
        @test nrow(unique(tripdata_large_filtered)) == 6242979
        @test length(gdf) == 6	
        @test collect(groupcols(gdf)) == [:passenger_count]
        @test Set(collect(valuecols(gdf))) == Set([:vendor_id, :pickup_datetime, :dropoff_datetime, :trip_distance, :pickup_longitude, :pickup_latitude, :rate_code, :store_and_fwd_flag, :dropoff_longitude, :dropoff_latitude, :payment_type, :fare_amount, :surcharge, :mta_tax, :tip_amount, :tolls_amount, :total_amount, :is_distance_longer_than_mean])
	@test res_collected[:, :n_trips] == [15368, 1116069, 4510158, 601379, 2, 3] * n_repeats
        @test res_collected[:, :total_trip_distance] == [4239, 325916, 1320006, 178700, 1, 2] * n_repeats
        @test res_collected[:, :fraction] == [0.28, 0.29, 0.3, 0.5, 0.67]
        
    end
end
