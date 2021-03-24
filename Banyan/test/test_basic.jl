@testset "Basic" begin
    run(
        "Creating/Destroying Jobs",
        () -> begin
            configure(;
                username = "BanyanTest",
                api_key = "7FBKWAv3ld0eOfghSwhX_g",
                ec2_key_pair_name = "EC2ConnectKeyPairTest",
                region = "us-west-2",
            )
        end,
    )

    run(
        "Creating/Destroying Jobs",
        () -> begin
            j = Job(;
                cluster_name = "banyancluster",
                nworkers = 2,
                banyanfile_path = "file:///home/ec2-user/banyan-julia/Banyan/res/Banyanfile.json",
            )
        end,
    )
end
