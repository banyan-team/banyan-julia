using Random

@testset "Get clusters" begin
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    clusters = get_clusters()
    get_cluster_s3_bucket_name(cluster_name)
    running_clusters = get_running_clusters()

    @test haskey(clusters, cluster_name)
    @test all(c -> c[2].status == :running, running_clusters)

end

@testset "Update clusters" begin
    cluster_name = ENV["BANYAN_CLUSTER_NAME"]

    update_cluster(cluster_name)
    cluster_status = get_cluster_status(cluster_name)

    @test cluster_status == :updating
    
    while cluster_status == :updating
        sleep(5)
        cluster_status = get_cluster_status(cluster_name)
    end
    @test cluster_status == :running
end

function bucket_exists(s3_bucket_name)
    ispath(S3Path("s3://$(s3_bucket_name)", config=Banyan.global_aws_config()))
end

@testset "Create clusters" begin
    Random.seed!()
    cluster_name = "cluster-$(Random.randstring(['a':'z'; '0':'9'], 6))"
    create_cluster(
        name=cluster_name,
        instance_type="t3.xlarge",  # 4 vCPUs
        max_num_workers=32,
        initial_num_workers=8,
        min_num_workers=2,
        scaledown_time=30,
    )
    @test c.status == :running
    delete_cluster(cluster_name)
end

@testset "Destroy and delete clusters with $s3_bucket S3 bucket" for s3_bucket in [
        "default", "user-provided"
    ]
    Random.seed!()
    cluster_name = "cluster-$(Random.randstring(['a':'z'; '0':'9'], 6))"
    if s3_bucket == "default"
        s3_bucket = nothing
    elseif s3_bucket == "user-provided"
        s3_bucket = Random.randstring(['a':'z'; '0':'9'], 6)
        s3_create_bucket(Banyan.global_aws_config(), s3_bucket)
    end

    # Create a cluster (at least initiate) and check that S3 bucket exists
    c = create_cluster(
        name=cluster_name,
        instance_type="t3.large",
        s3_bucket_name=s3_bucket,
        wait_now=false
    )
    sleep(30) # Just to ensure that cluster creation has initiated
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    s3_bucket_exists = bucket_exists(s3_bucket_name)
    if !isnothing(s3_bucket)
        @test s3_bucket == s3_bucket_name
    end
    @test s3_bucket_exists

    # Destroy cluster and check that S3 bucket still exists
    destroy_cluster(cluster_name)
    s3_bucket_exists = bucket_exists(s3_bucket_name)
    @test s3_bucket_exists
    sleep(30) # Just to ensure that cluster destruction is complete

    # Re-create cluster and check that S3 bucket exists and is same as before
    while get_cluster_status(cluster_name) != :terminated
        sleep(15)
    end
    c_r = create_cluster(
        name=cluster_name,
        wait_now=false
    )
    s3_bucket_name_r = get_cluster_s3_bucket_name(cluster_name)
    s3_bucket_exists = bucket_exists(s3_bucket_name_r)
    @test s3_bucket_exists
    @test s3_bucket_name == s3_bucket_name_r

    # Delete cluster
    delete_cluster(cluster_name)
    sleep(30)  # Just to ensure that bucket has been deleted
    s3_bucket_exists = bucket_exists(s3_bucket_name_r)
    @test !s3_bucket_exists

    # Check that the cluster cannot be created again
    @test_throws ErrorException create_cluster(name=cluster_name, wait_now=false)
end

@testset "Benchmark create_cluster with $instance_type instance type" for instance_type in [
    "t3.xlarge", "t3.2xlarge", "c5.2xlarge", "m4.4xlarge", "m4.10xlarge"
]
    Random.seed!()
    cluster_name = "cluster-$(Random.randstring(['a':'z'; '0':'9'], 6))"
    t = @elapsed begin
        c = create_cluster(
            name=cluster_name,
            instance_type=instance_type,
            max_num_workers=16,
            initial_num_workers=1
        )
    end
    delete_cluster(cluster_name)

    # Save results to file
    open("create_cluster_times.txt", "a") do f
        write(f, "$(instance_type)\t$(string(t/60))\n")
    end

    # Verify that cluster was spun up
    @test c.status == :running
end

@testset "Upload file to S3 from $src_type" for src_type in [
    "local",
    "http",
    "s3"
]
    if src_type == "local"
        src_path = "file://data/iris.csv"
        dst_name = "iris_from_local.csv"
    elseif src_type == "http"
        src_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
        dst_name = "iris_from_http.csv"
    elseif src_type == "s3"
        s3_bucket = Random.randstring(['a':'z'; '0':'9'], 6)
        dst_name = "data_from_s3"
        src_path = "s3://$s3_bucket/$dst_name"
        # Create a bucket and upload data
        s3_create_bucket(Banyan.global_aws_config(), s3_bucket)
        s3_put(Banyan.global_aws_config(), s3_bucket, dst_name, "some file contents")
    end

    cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    upload_to_s3(src_path; dst_name=dst_name, cluster_name=cluster_name)

    cluster_s3_bucket = get_cluster_s3_bucket_name(cluster_name)
    @test ispath(S3Path("s3://$cluster_s3_bucket/$dst_name"))

    # Cleanup
    s3_delete(Banyan.global_aws_config(), cluster_s3_bucket, dst_name)
    if src_type == "s3"
        s3_delete(Banyan.global_aws_config(), s3_bucket, dst_name)
        s3_delete_bucket(Banyan.global_aws_config(), s3_bucket)
    end
end

@testset "Upload dir to S3 from $src_type" for src_type in [
    "local",
]
    if src_type == "local"
        src_path = "data"
    end

    cluster_name = ENV["BANYAN_CLUSTER_NAME"]
    dst_name = upload_to_s3(src_path; cluster_name=cluster_name)

    cluster_s3_bucket = get_cluster_s3_bucket_name(cluster_name)
    @test ispath(S3Path("s3://$cluster_s3_bucket/$dst_name"))
    for f_name in readdir(src_path)
        @test ispath(S3Path("s3://$cluster_s3_bucket/$dst_name/$f_name"))
    end

    # Cleanup
    for f_name in readdir(src_path)
        s3_delete(Banyan.global_aws_config(), cluster_s3_bucket, "$dst_name/$f_name")
    end
end