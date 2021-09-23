# TODO:
# - Test both exact and inexact samples
# - Read from HDF5, CSV, Parquet, Arrow files, CSV datasets
# - Read from both S3 and Internet
# - Test shuffled, similar files, default, invalidated location but reused sample
# - Verify no error, length of returned sample, (eventually) ensure that rows come from the right files
@testset "Sample collection from $src_name" for src_name in
                                                ["iris_small.parquet", "iris_big.parquet"]
    use_job_for_testing()

end
