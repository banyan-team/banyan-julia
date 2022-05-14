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
    max_exact_sample_length = exact_or_inexact == "Exact" ? 1_024_000 : 0
    use_session_for_testing(sample_rate = 2) do

        # Use data to collect a sample from
        src_name = use_data(file_extension, on, single_file)

        # Construct location
        if reusing != "nothing"
            RemoteTableSource(src_name, invalidate_metadata = true, invalidate_sample = true)
            RemoteTableSource(src_name, metadata_invalid = true, sample_invalid = true)
        end
        remote_source = RemoteTableSource(
            src_name,
            metadata_invalid = (reusing == "nothing" || reusing == "sample"),
            sample_invalid = (reusing == "nothing" || reusing == "location"),
            shuffled = with_or_without_shuffled == "with",
            max_exact_sample_length = max_exact_sample_length
        )

        # Verify the location
        
        @test remote_source.total_memory_usage > 0
        @test !remote_source.parameters_invalid
        @test !remote_source.sample_invalid
        @test remote_source.src_parameters["nrows"] == src_nrows
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
