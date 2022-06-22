# TODO:
# - Test both exact and inexact samples
# - Read from HDF5, CSV, Parquet, Arrow files, CSV datasets
# - Read from both S3 and Internet
# - Test with and without S3FS
# - Test shuffled, similar files, default, invalidated location but reused sample or reused location
# - Verify no error, length of returned sample, location files and their nrows,
# (eventually) ensure that rows come from the right files
@testset "$exact_or_inexact sample collected from $file_extension $(single_file ? "file" : "directory") on $on S3FS $with_or_without_shuffled shuffled reusing $reusing" for exact_or_inexact in
                                                                                                                                                                                                [
        "Exact",
        "Inexact",
    ],
    (file_extension, single_file, on, src_nrows) in [
        ("h5", true, "S3", 20),
        ("h5", true, "Internet", 10),
    ],
    with_or_without_shuffled in ["with", "without"],
    reusing in ["nothing", "sample", "location", "sample and location"]

    # Use session with appropriate sample collection configuration
    max_exact_sample_length = exact_or_inexact == "Exact" ? 1_024_000 : 0
    use_session_for_testing(sample_rate = 2) do

        # Use data to collect a sample from
        src_name = use_data(on)

        # Construct location
        if reusing != "nothing"
            RemoteHDF5Source(src_name, invalidate_metadata = true, invalidate_sample = true)
            RemoteHDF5Source(src_name, metadata_invalid = true, sample_invalid = true)
        end
        remote_source = RemoteHDF5Source(
            src_name,
            metadata_invalid = (reusing == "nothing" || reusing == "sample"),
            sample_invalid = (reusing == "nothing" || reusing == "location"),
            shuffled = with_or_without_shuffled == "with",
            max_exact_sample_length = max_exact_sample_length
        )

        # Verify the location
        if contains(src_name, "h5")
            @test remote_source.src_parameters["ndims"] == 2
            @test !contains(remote_source.src_parameters["path"], "DS1")
            @test remote_source.src_parameters["subpath"] == "DS1"
            @test remote_source.src_parameters["size"][1] == src_nrows
        end

        # TODO: Add these tests
        # TODO: Fix sample collection in the optimizations/reuse and nbytes

        # Verify the sample
        sample_nrows = size(remote_source.sample.value, 1)
        if reusing == "nothing"
            if exact_or_inexact == "Exact"
                @test sample_nrows == src_nrows
            else
                @test sample_nrows == if isodd(src_nrows / 2)
                    cld(src_nrows, 2 * 2) * 2
                else
                    cld(src_nrows, 2)
                end
            end
        end
    end
end
