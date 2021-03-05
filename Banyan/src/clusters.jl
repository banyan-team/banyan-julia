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

function create_cluster(cluster_id)
    @debug "Creating cluster"
    # TODO: Load pt_lib_info correctly
    # TODO: Parameterize this function...
    send_request_get_response(
        :create_cluster,
        Dict{String, Any}(
            "cluster_id" => cluster_id,
            "username" => "BanyanTest",
            "ec2_key_pair" => "EC2ConnectKeyPairTest",
            "pcluster_additional_policy" => "arn:aws:iam::338603620317:policy/pcluster-additional-policy",
            "num_nodes" => 4,
            "instance_type" => "t3.large", #"c5.2xlarge",
            "s3_read_write_resource" => "arn:aws:s3:::banyanexecutor*",
            "banyanfile" => JSON.json(
                Dict(
		    "include" => [],
		    "banyanfile" => Dict(
		        "language" => "jl",
			"cluster" => Dict(
		            "scripts" => [],
			    "packages" => ["DataFrames"],
			    "pt_lib_info" => Dict("splits" => Dict(), "merges" => Dict(), "casts" => Dict()),
			    "pt_lib" => ""
			),
	                "job" => Dict()
		    )
		)
            )
        ),
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
