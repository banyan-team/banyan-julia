using Base64
using JSON

function encode_config_file(config_file_path)
    f = open(config_file_path, "r")
    io = IOBuffer()
    iob64_encode = Base64EncodePipe(io)
    write(iob64_encode, read(f, String))
    close(iob64_encode)
    return String(take!(io))
end

function create_cluster(cluster_id, config_file_path)
    @debug "Creating cluster"
    config_file_path = joinpath(@__DIR__, "../res/pcluster_config_test")
    pcluster_config = encode_config_file(config_file_path)
    # TODO: Load pt_lib_info correctly
    # TODO: Parameterize this function...
    send_request_get_response(
        :create_cluster,
	Dict(
	     "cluster_id" => cluster_id,
	     "name" => "BanyanTest",
	     "ec2_key_pair" => "EC2ConnectKeyPairTest",
	     "pcluster_additional_policy" => "arn:aws:iam::338603620317:policy/pcluster-additional-policy",
	     "num_nodes" => 2,
	     "instance_type" => "c5.2xlarge",
	     "banyanfile" => JSON.json(Dict("language" => "jl", "cluster" => Dict("packages" => ["DataFrames"], "pt_lib_info" => Dict("splits" => Dict(), "merges" => Dict(), "casts" => Dict())), "job" => Dict()))
	)
    )

end

function destroy_cluster(cluster_id, config_file_path)
    @debug "Destroying cluster"

    pcluster_config = encode_config_file(config_file_path)

    send_request_get_response(
        :destroy_cluster,
        Dict(
            "cluster_id" => cluster_id,
            "pcluster_config" => pcluster_config
	)
    )
end
