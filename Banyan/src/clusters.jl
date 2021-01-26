using Base64

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
    config_file_path = joinpath(@__DIR__, "../res/pcluster_config2")
    pcluster_config = encode_config_file(config_file_path)
    # TODO: Load pt_lib_info correctly
    send_request_get_response(
        :create_cluster,
	Dict(
            "cluster_id" => cluster_id,
	    "language" => "jl",
	    "pt_lib_info" => Dict(),
	    "pcluster_config" => pcluster_config
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
