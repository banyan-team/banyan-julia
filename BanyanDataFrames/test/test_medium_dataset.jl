function upload_tripdata_to_s3(bucket_name)
    verify_file_in_s3(
        bucket_name,
        "tripdata.csv",
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv",
    )
end

@testset "Complex usage of BanyanDataFrames on medium dataset" begin
    run_with_job("Filter groupby") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_tripdata_to_s3(bucket)
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")

        # Read data and get properties
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")
        tripdata = dropmissing(tripdata)
        @test nrow(tripdata) == 10906858
        @test ncol(tripdata) == 19
        @test size(tripdata) == (10906858, 19)
        @test collect(names(tripdata))[1] == "VendorID"
        @test collect(propertynames(tripdata))[1] == :VendorID

        # Get all trips with distance longer than 1.0, group by passenger count,
        # and get the average trip distance for each group
        distances = collect(
            combine(
                groupby(filter(row -> row.trip_distance > 1.0, tripdata), :passenger_count),
                :trip_distance => mean,
            ),
        )
        isapprox(first(distances)[:trip_distance_mean], 8.1954, atol = 1e-3)
        isapprox(last(distances)[:trip_distance_mean], 8.2757, atol = 1e-3)
    end

    run_with_job("Operations on DateTime") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_tripdata_to_s3(bucket)
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")

        # Compute the hour of the day which has the greatest average trip distance.
        setindex!(
            tripdata,
            :start_time,
            map(t -> DateTime(t, "yyyy-mm-dd HH:MM:SS"), tripdata[:, :tpep_pickup_datetime])
        )
        setindex!(
            tripdata,
	    :end_time,
	    map(t -> DateTime(t, "yyyy-mm-dd HH:MM:SS"), tripdata[:, :tpep_dropoff_datetime])
	)
        setindex!(
            tripdata,
	    :start_hour,
	    map(t -> hour(t), tripdata[:, :start_time])
	)
        tripdata_grouped = groupby(tripdata, :start_hour)
        means = combine(tripdata_grouped, :trip_distance => mean)
        means_sorted = collect(sort(means, :trip_distance_mean))
        @test round(means_sorted[:, :trip_distance_mean], digits=3) == [2.523, 2.529, 2.577, 2.597, 2.627, 2.753, 2.831, 2.875, 2.893, 2.904, 3.048, 3.247, 3.248, 3.329, 3.381, 3.469, 3.527, 3.824, 4.109, 4.603, 5.872, 6.081, 12.34, 21.456]
        @test means_sorted[:, :start_hour] == [9, 8, 18, 12, 19, 13, 7, 15, 16, 20, 21, 1, 2, 0, 23, 3, 6, 17, 4, 5, 11, 22, 10, 14]
        @test means_sorted[:, :start_hour][end] == 14
    
        # Get the first trip for each pickup time, sort by drop off time, and filter for trips with start times on/after Jan. 30.
        tripdata_unique_start = sort(unique(tripdata, :start_time), :end_time)
        tripdata_unique_start_end_of_month = tripdata_unique_start[map(st -> st > DateTime(2016, 1, 30, 00, 00, 00, 00), tripdata_unique_start[:, :start_time]), :]
        @test nrow(tripdata_unique_start) == 2368616
        @test (tripdata_unique_start_end_of_month) == 163601
        @test collect(round(sum(tripdata_unique_start_end_of_month))) == 490661
        pickup_res = collect(tripdata_unique_start_end_of_month[:, :tpep_pickup_datetime])
        @test pickup_res[1] == "2016-01-30 00:00:01"
        @test pickup_res[2] == "2016-01-30 00:00:02"
        @test pickup_res[163593] == "2016-01-31 23:57:26"
    end

    run_with_job("Groups and joins") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_tripdata_to_s3(bucket)
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")

        # Group by passenger count and compute min-max normalized trip distance. Get average for each group.
        gdf = groupby(tripdata, :passenger_count)

        # Method 1
        result_1 = transform(
	    gdf,
	    :trip_distance => x -> (x .- minimum(x)) ./ (maximum(x) - minimum(x))
	)
        result_1 = rename(
	    result_1,
	    :trip_distance_function => :trip_distance_normalized
	)
        result_1 = combine(groupby(result_1, :passenger_count), :trip_distance_normalized => mean)

        # Method 2
        min_max = combine(gdf, :trip_distance => minimum, :trip_distance => maximum)
        result_2 = innerjoin(tripdata, min_max, on = :passenger_count)
        result_2[:, :trip_distance_normalized] = map(
	    (d, min, max) -> (d - min) / (max - min),
	    result_2[:, :trip_distance],
	    result_2[:, :trip_distance_minimum],
	    result_2[:, :trip_distance_maximum]
	)
        result_2 = combine(groupby(result_2, :passenger_count), :trip_distance_normalized => mean)

        result_1 = collect(result_1)
        result_2 = collect(result_2)
        @test round.(result_1[:, :trip_distance_normalized_mean], digits=3) == round.(result_2[:, :trip_distance_normalized_mean], digits=3) == [0.031, 0.0, 0.0, 0.013, 0.0, 0.021, 0.042, 0.146, 0.181, 0.112]
    end

    run_with_job("Select and subset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_tripdata_to_s3(bucket)
        tripdata = read_csv("s3://$(bucket)/tripdata.csv")

        # Get trips which are longer than the average trip length for the day.
        setindex!(
            tripdata,
            :day,
            map(t -> day(DateTime(t, "yyyy-mm-dd HH:MM:SS")), tripdata[:, :tpep_pickup_datetime])
        )
        gdf = groupby(tripdata, :day)
        res = subset(
	    gdf,
            :trip_distance => d -> d .> mean(d)
	)

        # Save these trips to s3, with the pickup time, dropoff time, average trip distance for each day.
        res_2 = select(groupby(res, :day), :tpep_pickup_datetime, :tpep_dropoff_datetime, :trip_distance .=> mean)
        write_csv(res_2, "s3://$(bucket)/tripdata_new.csv")

        @test collect(combine(groupby(res, :passenger_count), nrow))[:, :nrow] == [69, 1813376, 403749, 109032, 53536, 149283, 88698, 6, 11, 3]
        @test collect(combine(groupby(res_2, :day)))[:, :day] == [103117, 83528, 80937, 120, 27639, 91998, 96279, 105021, 117609, 98359, 88022, 97655, 106757, 105257, 106580, 116446, 106687, 86604, 97973, 100627, 26725, 110904, 25868, 40623, 71419, 85354, 95915, 2477, 112558, 127809, 100896]
    end
end
