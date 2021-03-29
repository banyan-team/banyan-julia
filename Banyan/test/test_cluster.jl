@testset "Cluster Management" begin
    run(
        "Configuration",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-west-2",
            )
        end,
    )

    # TODO: Add tests for creating, destroying cluster and updating with more
    # complex Banyanfiles. The point of additional tests for updating is to
    # ensure that we parse, merge, and load Banyanfiles correctly and such
    # tests should cover all the different fields in a Banyanfile including
    # `includes` for example.

    run(
        "Updating Cluster",
        () -> begin
            update_cluster(
                name = "banyancluster",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
end


# Tests create_cluster and calls get_clusters to ensure correct behavior
#   expected_presence (bool): indicates whether the cluster should be listed
#   expected_status (bool): indicates expected status if cluster should be listed
#   kwargs : arguments for create_cluster
function test_create_cluster(expected_presence, expected_status;kwargs...)
    create_cluster(;kwargs...)
    clusters = get_clusters()
    @test haskey(clusters, name) == expected_presence
    if (haskey(clusters, kwargs[:name]))
        @test clusters[kwargs[:name]].status == expected_status
    end
end


@testset "Cluster Creation - Should Fail Immediately" begin
    run(
        "createcluster_bad_username",
        () -> begin
            configure(;
                username = "BadUser",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-west-2",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    run(
        "createcluster_bad_api_key",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "invalidapikey",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-west-2",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    run(
        "createcluster_bad_ec2_key_pair_name",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "NoEC2KeyPair",
                region = "us-west-2",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    run(
        "createcluster_bad_region",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "noregion",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    run(
        "createcluster_bad_instance_type",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-west-2",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "a1.metal",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    run(
        "createcluster_bad_banyanfile",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "uw-west-2",
            )
            test_create_cluster(
                false,
                "";
                name = "badcluster",
                instance_type = "t3.large",
                banyanfile_path = "file://res/banyanfile_badcluster.json",
            )
        end,
    )
end


@testset "Cluster Creation" begin
    run(
        "createcluster_region",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-east-1",
            )
            test_create_cluster(
                true,
                :creating;
                name = "cluster_useast1",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
            )
        end,
    )
    # run(
    #     "createcluster_s3bucket",
    #     () -> begin
    #         configure(;
    #             username = "BanyanTest",
    #             api_key = "7FBKWAv3ld0eOfghSwhX_g",
    #             ec2_key_pair_name = "EC2ConnectKeyPairTest",
    #             region = "us-east-1",
    #         )
    #         test_create_cluster(
    #             true,
    #             :creating,
    #             name = "cluster_useast1",
    #             instance_type = "t3.large",
    #             banyanfile_path = "file://res/Banyanfile.json",
    #             s3_bucket_arn::String = "TODOTODOTODTODOTO",
    #         )
    #     end,
    # )
    run(
        "createcluster_iam",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-east-1",
            )
            test_create_cluster(
                true,
                :creating;
                name = "cluster_useast1",
                instance_type = "t3.large",
                banyanfile_path = "file://res/Banyanfile.json",
                # s3_bucket_arn::String = "TODOTODOTODTODOTO",
            )
        end,
    )
end
