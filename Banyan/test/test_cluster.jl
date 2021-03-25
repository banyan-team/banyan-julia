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
