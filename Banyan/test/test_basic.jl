

@testset "Configure" begin
    configure(;
        username="BanyanTest",
	api_key="7FBKWAv3ld0eOfghSwhX_g",
	ec2_key_pair_name="EC2ConnectKeyPairTest",
	region="us-west-2"
    )
end

@testset "Create/Destroy Job" begin
    #j = Job()
    j = Job(;
        cluster_name="banyancluster",
        nworkers=2
    ) 
end
