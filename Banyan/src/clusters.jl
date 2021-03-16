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

function load_banyanfile(banyanfile_path::String)
    # TODO: don't hardcode this
    banyanfile_path = "res/Banyanfile.json"
    banyanfile = Dict()
    open(banyanfile_path, "r") do f
       global banyanfile
       banyanfile = JSON.parse(read(f, String))
    end
    # Load pt_lib_info if path provided
    pt_lib_info = banyanfile["banyanfile"]["cluster"]["pt_lib_info"]
    if typeof(pt_lib_info) == String
        open(pt_lib_info, "r") do f
            pt_lib_info = JSON.parse(read(f, String))
        end
        banyanfile["banyanfile"]["cluster"]["pt_lib_info"] = pt_lib_info
    end
    return banyanfile
end

function load_configfile(configfile_path::String)
    # TODO: don't hardcode this
    configfile_path = "res/BanyanConfig.json"
    configfile = Dict()
    open(configfile_path, "r") do f
        global configfile
        configfile = JSON.parse(read(f, String))
    end
    return configfile
end


# Required: cluster_id, num_nodes
function create_cluster(cluster_id::String, num_nodes::Int, instance_type::String, s3_read_write_resource::String, ec2_key_pair, pcluster_additional_policy)
    @debug "Creating cluster"
    # These two config files should always be the same location, or path should be passed in as parameters

    # Read config files

    banyanfile = load_banyanfile()
    configfile = load_configfile()

    if ec2_key_pair == nothing
        ec2_key_pair = configfile["ec2_key_pair"]
       configfile = Dict()
    end
    if pcluster_additional_policy == nothing
        pcluster_additional_policy = configfile["pcluster_additional_policy"]
    end

    username = configfile["username"]
  
    send_request_get_response(
        :create_cluster,
        Dict{String, Any}(
            "cluster_id" => cluster_id,
            "username" => username,
            "ec2_key_pair" => ec2_key_pair,
            "pcluster_additional_policy" => pcluster_additional_policy,
            "num_nodes" => num_nodes,
            "instance_type" => instance_type,  #"t3.large", "c5.2xlarge",
            "s3_read_write_resource" => s3_read_write_resource,  #"arn:aws:s3:::banyanexecutor*",
            "banyanfile" => JSON.json(banyanfile)
        ),
    )
end

function destroy_cluster(cluster_id::String)
    @debug "Destroying cluster"

    configfile = load_configfile()

    username = configfile["username"]

    send_request_get_response(
        :destroy_cluster,
        Dict(
            "cluster_id" => cluster_id,
            "username" => username
	)
    )
end

function update_cluster(cluster_id::String)
    @debug "Updating cluster"

     # Require restart: pcluster_additional_policy, s3_read_write_resource, num_workers
     # No restart: 

    configfile = load_configfile()

    username = configfile["username"]

    send_request_get_response(
        :update_cluster,
	Dict(
	     "cluster_id" => cluster_id,
	     "username" -> username
        )

    )
end
