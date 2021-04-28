#using HTTP
#using JSON
#using TOML

##############
# CONVERSION #
##############

jl_to_json(j) = JSON.json(j)

json_to_jl(j) = JSON.parse(j)

##################
# AUTHENTICATION #
##################

# TODO: Manage TOML file in joinpath(homedir(), ".banyan", "banyanconfig.toml")
# TODO: 
# TODO: Functions for setting

global banyan_config = nothing
global aws_config_by_region = Dict()

function load_config()
    global banyan_config

    banyanconfig_path = joinpath(homedir(), ".banyan", "banyanconfig.toml")
    if isfile(banyanconfig_path)
        banyan_config = TOML.parsefile(banyanconfig_path)
    end
end

function write_config()
    global banyan_config

    # Write to banyanconfig.toml
    banyanconfig_path = joinpath(homedir(), ".banyan", "banyanconfig.toml")
    mkpath(joinpath(homedir(), ".banyan"))
    f = open(banyanconfig_path, "w")
    TOML.print(f, banyan_config)
    close(f)
end

if_in_or(key, obj, el = nothing) =
    if key in keys(obj)
        obj[key]
    else
        el
    end

function configure(; kwargs...)
    # Load arguments
    kwargs = Dict(kwargs)
    username = if_in_or(:username, kwargs)
    user_id = if_in_or(:user_id, kwargs)
    api_key = if_in_or(:api_key, kwargs)
    ec2_key_pair_name = if_in_or(:ec2_key_pair_name, kwargs)
    region = if_in_or(:region, kwargs)
    require_ec2_key_pair_name =
        if_in_or(:require_ec2_key_pair_name, kwargs, false)

    # Initialize
    global banyan_config
    is_modified = false
    is_valid = true


    # Ensure a configuration has been created or can be created. Otherwise,
    # return nothing
    if isnothing(banyan_config)
        if !isnothing(user_id) && !isnothing(api_key)
            banyan_config = Dict(
                "banyan" =>
                    Dict("username" => username, "user_id" => user_id, "api_key" => api_key),
                "aws" => Dict(),
            )
            is_modified = true
        else
            error("User ID and API key not provided")
        end
    end

    # Check for changes in required
    if !isnothing(username) &&
       (username != banyan_config["banyan"]["username"])
        banyan_config["banyan"]["username"] = username
        is_modified = true
    end
    if !isnothing(user_id) &&
        (user_id != banyan_config["banyan"]["user_id"])
         banyan_config["banyan"]["user_id"] = user_id
         is_modified = true
     end
    if !isnothing(api_key) && (api_key != banyan_config["banyan"]["api_key"])
        banyan_config["banyan"]["api_key"] = api_key
        is_modified = true
    end

    # Check for changes in potentially required

    # aws.ec2_key_pair_name
    if !isnothing(ec2_key_pair_name) && (
        !(haskey(banyan_config["aws"], "ec2_key_pair_name")) ||
        ec2_key_pair_name != banyan_config["aws"]["ec2_key_pair_name"]
    )
        banyan_config["aws"]["ec2_key_pair_name"] = ec2_key_pair_name
        is_modified = true
    end
    if require_ec2_key_pair_name &&
       !("ec2_key_pair_name" in banyan_config["aws"])
        error("Name of an EC2 key pair required but not provided")
    end

    # aws.region
    if !isnothing(region) && (
        !(haskey(banyan_config["aws"], "region")) ||
        region != banyan_config["aws"]["region"]
    )
        banyan_config["aws"]["region"] = region
        is_modified = true
    end

    # Update config file if it was modified
    if is_modified
        write_config()  #update_config()
    end

    return banyan_config
end

function get_aws_config(region::String)
    global aws_config_by_region
    configure(region = region)
    if !(region in keys(aws_config_by_region))
        # println("region = ", region)
        aws_config_by_region[region] = aws_config(region = region)
    end
    aws_config_by_region[region]
end

function get_aws_config()
    global aws_config_by_region
    try
        get_aws_config(configure()["aws"]["region"])
    catch e
        @warn "Using default AWS region of us-west-2 in \$HOME/.banyan/banyanconfig.toml"
        configure(region = "us-west-2")
        get_aws_config(configure()["aws"]["region"])
    end
end

get_aws_config_region() = get_aws_config()[:region]

#########################
# ENVIRONMENT VARIABLES #
#########################

is_debug_on() = "JULIA_DEBUG" in keys(ENV) && ENV["JULIA_DEBUG"] == "all"

macro in_env(key)
    return :(string("BANYAN_", getpid(), "_", $key) in keys(ENV))
end

macro env(key)
    return :(ENV[string("BANYAN_", getpid(), "_", $key)])
end

macro delete_in_env(key)
    return :(delete!(ENV, string("BANYAN_", getpid(), "_", $key)))
end

################
# API REQUESTS #
################

method_to_string(method) = begin
    if method == :create_cluster
        "create-cluster"
    elseif method == :destroy_cluster
        "destroy-cluster"
    elseif method == :describe_clusters
        "describe-clusters"
    elseif method == :create_job
        "create-job"
    elseif method == :destroy_job
        "destroy-job"
    elseif method == :evaluate
        "evaluate"
    elseif method == :update_cluster
        "update-cluster"
    end
end

"""
Sends given request with given content
"""
function send_request_get_response(method, content::Dict)
    # Prepare request
    # content = convert(Dict{Any, Any}, content)
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]
    # TODO: Allow content["debug"]
    # content["debug"] = is_debug_on()
    url = string(BANYAN_API_ENDPOINT, method_to_string(method))
    headers = (
        ("content-type", "application/json"),
        ("Username-APIKey", "$user_id-$api_key"),
    )

    # Post and return response
    try
        # println(headers)
	    # println(content)
        response = HTTP.post(url, headers, JSON.json(content))
        # println(response)
        body = String(response.body)
        return JSON.parse(body)
        #return JSON.parse(JSON.parse(body)["body"])
    catch e
        if e isa HTTP.ExceptionRequest.StatusError
            if e.response.status == 403
                throw(
                    ErrorException(
                        "Please set a valid api_key. Sign in to the dashboard to retrieve your api key.",
                    ),
                )
            end
            if e.response.status != 504
                throw(ErrorException(String(take!(IOBuffer(e.response.body)))))
            elseif method == :create_cluster
                # println(
                #     "Cluster creation in progress. Please check dashboard to view status.",
                # )
            elseif method == :create_job
                # println(
                #     "Job creation in progress. Please check dashboard to view status.",
                # )
            elseif method == :evaluate
                # println(
                #     "Evaluation is in progress. Please check dashboard to view status.",
                # )
            end
        else
            rethrow()
        end
    end
end
