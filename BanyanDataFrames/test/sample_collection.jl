# TODO:
# - Test both exact and inexact samples
# - Read from HDF5, CSV, Parquet, Arrow files, CSV datasets
# - Read from both S3 and Internet
# - Test with and without S3FS
# - Test shuffled, similar files, default, invalidated location but reused sample or reused location
# - Verify no error, length of returned sample, location files and their nrows,
# (eventually) ensure that rows come from the right files
@testset "$exact_or_inexact sample collected from $file_extension $(single_file ? "file" : "directory") on $on $with_or_without_shuffled shuffled reusing $reusing" for exact_or_inexact in
                                                                                                                                                                                                [
        "Exact",
        "Inexact",
    ],
    (file_extension, single_file, on, src_nrows) in [
        ("csv", true, "S3", 150),
        ("parquet", true, "S3", 150),
        ("arrow", true, "S3", 150),
        ("csv", true, "Internet", 150),
        ("parquet", true, "Internet", 150),
        ("arrow", true, "Internet", 150),
        ("csv", false, "S3", 150 * 20),
        ("parquet", false, "S3", 150 * 20),
        ("arrow", false, "S3", 150 * 20),
    ],
    with_or_without_shuffled in ["with", "without"],
    reusing in ["nothing", "sample", "location", "sample and location"]

    # Use session with appropriate sample collection configuration
    use_session_for_testing(sample_rate = 2) do

        # Use data to collect a sample from
        src_name = use_data(file_extension, on, single_file)

        # Construct location
        if reusing != "nothing"
            RemoteTableSource(src_name)
            invalidate_location(src_name)
            RemoteTableSource(src_name)
        end
        if (reusing == "nothing" || reusing == "sample")
            invalidate_metadata(src_name)
        end
        if (reusing == "nothing" || reusing == "location")
            invalidate_sample(src_name)
        end
        configure_sampling(
            always_exact = exact_or_inexact,
            assume_shuffled = with_or_without_shuffled == "with",
        )
        remote_source = RemoteTableSource(src_name)

        # Verify the location
        
        @test remote_source.sample_memory_usage > 0
        @test !remote_source.metadata_invalid
        @test !remote_source.sample_invalid
        @test remote_source.src_parameters["nrows"] == string(src_nrows)
        # if contains(src_name, "dir")
        #     @test length(remote_source.files) == 10
        #     for f in remote_source.files
        #         @test f["nrows"] == 150
        #     end
        # else
        #     @test length(remote_source.files) == 1
        # end

        # TODO: Add these tests
        # TODO: Fix sample collection in the optimizations/reuse and nbytes

        # Verify the sample
        sample_nrows = nrow(remote_source.sample.value)
        if reusing == "nothing"
            if exact_or_inexact == "Exact"
                @test sample_nrows == src_nrows
            else
                @test sample_nrows == cld(src_nrows, 2)
            end
        end
    end
end

@testset "Reading/writing $(shuffled ? "shuffled " : "")$format data and sampling it with $scheduling_config and a maximum of $max_num_bytes bytes for exact sample" for scheduling_config in
    [
        "default scheduling",
        "parallelism encouraged",
        "parallelism and batches encouraged",
    ],
    format in ["csv", "parquet"],
    max_num_bytes in [0, 100_000_000_000],
    shuffled in [true, false]

    use_session_for_testing(scheduling_config_name = scheduling_config) do
        use_basic_data()

        bucket = get_cluster_s3_bucket_name()

        configure_sampling(max_num_bytes_exact=max_num_bytes, always_shuffled=shuffled, for_all_locations=true, default=true)
        exact_sample = max_num_bytes > 0

        invalidate_all_locations()

        p1 = "s3://$(bucket)/iris_large.$format"
        p2 = "s3://$(bucket)/iris_large_tmp.$format"

        df = read_table(p1; metadata_invalid=true, invalidate_samples=true)
        sample(df)

        configure_sampling(p2; sample_rate=5)
        write_table(df, p2)
        @test get_sample_rate(p2) == 5
        @test has_metadata(p2)
        # NOTE: We don't compute _exact_ samples on writing
        @test has_sample(p2) == !exact_sample
        invalidate_metadata(p2)
        @test !has_metadata(p2)
        @test has_sample(p2) == !exact_sample
        invalidate_location(p2)
        @test !has_metadata(p2)
        @test !has_sample(p2)

        df2 = read_table(p2)
        sample(df2)
        df2 = read_table(p2; samples_invalid=true)
        sample(df2)
        @test get_sample_rate(p2) == 5
        configure_sampling(sample_rate=7, for_all_locations=true)
        @test get_sample_rate(p2) == 5
        df2 = read_table(p2; metadata_invalid=true)
        sample(df2)
        @test get_sample_rate(p2) == 5
        @test get_sample_rate() == 7
        configure_sampling(sample_rate=7, for_all_locations=true)
        @test get_sample_rate(p2) == 5
        configure_sampling(sample_rate=7, force_new_sample_rate=true, for_all_locations=true)
        @test get_sample_rate(p2) == 7
        @test get_sample_rate() == 7
        df2 = read_table(p2)
        @test get_sample_rate(p2) == 7
        @test get_sample_rate() == 7
        df2 = read_table(p2; location_invalid=true)
        sample(df2)
        @test has_metadata(p2)
        @test has_sample(p2)
        configure_sampling(p2; always_exact=true)
        sample(df2)
    end
end
