@testset "Stress test of BanyanDataFrames on a large dataset" begin
    run_with_job("") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        iris = read_csv("s3://$(bucket)/flights.csv")
    end
end
