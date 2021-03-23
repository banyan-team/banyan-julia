@testset "Configure" begin
    configure(;
        username="BanyanTest",
        api_key="7FBKWAv3ld0eOfghSwhX_g",
        ec2_key_pair_name="EC2ConnectKeyPairTest",
        region="us-west-2"
    )
end

@testset "Update cluster" begin
    update_cluster(;
        name="banyancluster",
	banyanfile_path="banyan-julia/Banyan/res/banyanfile.json"
    )
end
