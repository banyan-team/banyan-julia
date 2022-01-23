# TODO:
# - Test both exact and inexact samples
# - Read from HDF5, CSV, Parquet, Arrow files, CSV datasets
# - Read from both S3 and Internet
# - Test with and without S3FS
# - Test shuffled, similar files, default, invalidated location but reused sample or reused location
# - Verify no error, length of returned sample, location files and their nrows,
# (eventually) ensure that rows come from the right files
@testset "$exact_or_inexact sample collected from $file_extension $(single_file ? "file" : "directory") on $on $with_or_without_s3fs S3FS $with_or_without_shuffled shuffled reusing $reusing" for exact_or_inexact in
                                                                                                                                                                                                [
        "Exact",
        "Inexact",
    ],
    (file_extension, single_file, on, src_nrows) in [
        ("h5", true, "S3", 10),
        ("h5", true, "Internet", 10),
    ],
    with_or_without_shuffled in ["with", "without"],
    reusing in ["nothing", "sample", "location", "sample and location"],
    with_or_without_s3fs in ["with", "without"]

    # Use job with appropriate sample collection configuration
    use_job_for_testing(
        sample_rate = 2,
        max_exact_sample_length = exact_or_inexact == "Exact" ? 1_024_000 : 0,
        with_s3fs = with_or_without_s3fs == "with",
    ) do

        # Use data to collect a sample from
        src_name = use_data(file_extension, on, single_file)

        # Construct location
        if reusing != "nothing"
            Remote(src_name, source_invalid = true, sample_invalid = true)
        end
        remote_source = RemoteHDF5Source(
            src_name,
            source_invalid = (reusing == "nothing" || reusing == "sample"),
            sample_invalid = (reusing == "nothing" || reusing == "location"),
            shuffled = with_or_without_shuffled == "with",
        )

        # Verify the location
        if contains(src_name, "h5")
            @test remote_source.ndims == 2
            @test !contains(remote_source.path, "DS1")
            @test remote_source.subpath == "DS1"
            @test remote_source.size[1] == src_nrows
        else
            @test remote_source.nbytes > 0
            @test remote_source.nrows == src_nrows

            if contains(src_name, "dir")
                @test length(remote_source.files) == 10
                for f in remote_source.files
                    @test f["nrows"] == 150
                end
            else
                @test length(remote_source.files) == 1
            end
        end

        # TODO: Add these tests
        # TODO: Fix sample collection in the optimizations/reuse and nbytes

        # Verify the sample
        sample_nrows =
            contains(src_name, "h5") ? size(remote_source.sample.value, 1) :
            nrow(remote_source.sample.value)
        if exact_or_inexact == "Exact"
            @test sample_nrows == src_nrows
        else
            @test sample_nrows == cld(src_nrows, 2)
        end
    end
end