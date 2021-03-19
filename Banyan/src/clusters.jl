using Base64
using JSON


function load_banyanfile(banyanfile_path::String)
    banyanfile_path = "res/Banyanfile.json"  # TODO: Remove this
    banyanfile = Dict()
    open(banyanfile_path, "r") do f
       global banyanfile
       banyanfile = JSON.parse(read(f, String))
    end
    # Load all included banyanfiles
    include = []
    for inc in banyanfile["include"]
        if inc isa String
            # TODO: Determine wheter to download or is local path
            # download(inc)
            open(inc, "r") do f
                append!(include, JSON.parse(read(f, String)))
            end
        else
            append(!include, inc)
        end
    end
    # Load pt_lib_info if path provided
    pt_lib_info = banyanfile["require"]["cluster"]["pt_lib_info"]
    if typeof(pt_lib_info) == String
        # TODO: Determine whether to download or is local path
            # download(pt_lib_info)
        open(pt_lib_info, "r") do f
            banyanfile["require"]["cluster"]["pt_lib_info"] = JSON.parse(read(f, String))
        end
    end
    return banyanfile
end

function load_configfile(configfile_path::String)
    configfile_path = "res/BanyanConfig.json"  # TODO: Remove this
    configfile = Dict()
    open(configfile_path, "r") do f
        global configfile
        configfile = JSON.parse(read(f, String))
    end
    return configfile
end


# Required: cluster_id, num_nodes
function create_cluster(
    cluster_id::String,
    num_nodes::Int,
    instance_type::String,
    ec2_key_pair::String,
    pcluster_additional_policy::String,
    s3_read_write_resource::String,
    banyanfile_path::String,
    configfile_path::String
    )
    @debug "Creating cluster"

    # Read config files

    banyanfile = load_banyanfile(banyanfile_path)
    configfile = load_configfile(configfile_path)

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

# TODO: Make parameters optional
function update_cluster(
    cluster_id::String,
    pcluster_additional_policy::String,
    s3_read_write_resource::String,
    max_nodes::Int,
    banyanfile_path::String,
    configfile_path::String
    )
    @debug "Updating cluster"

     # Require restart: pcluster_additional_policy, s3_read_write_resource, num_nodes
     # No restart: Banyanfile

    configfile = load_configfile(configfile_path)

    username = configfile["username"]

    banyanfile = load_banyanfile(banyanfile_path)

    send_request_get_response(
        :update_cluster,
	Dict(
	     "cluster_id" => cluster_id,
	     "username" => username,
         "pcluster_additional_policy" => pcluster_additional_policy,
         "s3_read_write_resource" => s3_read_write_resource,
         "max_nodes" => max_nodes,
         "banyanfile" => banyanfile
        )

    )
end
