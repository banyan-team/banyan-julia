##############
# CONVERSION #
##############

# NOTE: `jl` referes to a subset of Julia that can be serialized to or
# deserialized from JSON with ease

jl_to_json(j) = JSON.json(j)

json_to_jl(j) = JSON.parse(j)

key_to_jl(key) = reinterpret(UInt8, hash(string(key))) |> String
axis_to_jl(axis) = reinterpret(UInt8, hash(string(key))) |> String

total_memory_usage(val) =
    begin
        size = Base.summarysize(val)
        # TODO: Maybe make this larger
        if size ≤ 128
            0
        else
            size
        end
    end

to_vector(v::Vector) = v
to_vector(v) = [v]

function ndiv(x, by; dims)
    x = [x...]
    x[dims] = div(x[dims], by)
    Tuple(x)
end

##################
# AUTHENTICATION #
##################

# Process-local configuration for the account being used. It wouldn't be hard
# to but there shouldn't be any reason to make this thread-local (since only
# one account should be being used per workstation or per server where
# Banyan.jl may be being used). However, wrapping this in a mutex to ensure
# synchronized mutation in this module would be a good TODO.
global banyan_config = nothing
global aws_config_in_usage = nothing

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
    # This function allows for users to configure their authentication.
    # Authentication details are then saved in
    # `$HOME/.banyan/banyanconfig.toml` so they don't have to be entered in again
    # each time a program using the Banyan client library is run

    # Load arguments
    kwargs = Dict(kwargs)
    username = if_in_or(:username, kwargs)
    user_id = if_in_or(:user_id, kwargs)
    api_key = if_in_or(:api_key, kwargs)
    ec2_key_pair_name = if_in_or(:ec2_key_pair_name, kwargs)
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
        error("Name of an EC2 key pair required but not provided; visit here to create a key pair: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair")
    end

    # # aws.region
    # if !isnothing(region) && (
    #     !(haskey(banyan_config["aws"], "region")) ||
    #     region != banyan_config["aws"]["region"]
    # )
    #     banyan_config["aws"]["region"] = region
    #     is_modified = true
    # end

    # Update config file if it was modified
    if is_modified
        write_config()  #update_config()
    end

    return banyan_config
end

function get_aws_config()
    global aws_config_in_usage

    # Get AWS configuration
    if isnothing(aws_config_in_usage)
        # Get region according to ENV, then credentials, then config files
        profile = get(ENV, "AWS_PROFILE", get(ENV, "AWS_DEFAULT_PROFILE", "banyan_nothing"))
        env_region = get(ENV, "AWS_DEFAULT_REGION", "")
        credentialsfile = read(Inifile(), joinpath(homedir(), ".aws", "credentials"))
        configfile = read(Inifile(), joinpath(homedir(), ".aws", "config"))
        credentials_region = _get_ini_value(credentialsfile, profile, "region", default_value="")
        config_region = _get_ini_value(configfile, profile, "region", default_value="")

        # Choose the region that is not default
        region = env_region
        region = isempty(region) ? credentials_region : region
        region = isempty(region) ? config_region : region

        println(region)

        if isempty(region)
            throw(Exception("No AWS region specified"))
        end

        aws_config_in_usage = Dict(
            :creds => AWSCredentials(),
            :region => region
        )
    end

    # # Use default location if needed
    # if !haskey(aws_config_in_usage, :region)
    #     @warn "Using default AWS region of us-west-2 in \$HOME/.banyan/banyanconfig.toml"
    #     aws_config_in_usage[:region] = "us-west-2"
    # end

    # Convert to dictionary and return

    aws_config_in_usage
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

##########################################
# Ordering hash for computing  divisions #
##########################################

# NOTE: `orderinghash` must either return a number or a vector of
# equally-sized numbers

# NOTE: This is duplicated between pt_lib.jl and the client library
orderinghash(x::Any) = x # This lets us handle numbers and dates
orderinghash(s::String) = Integer.(codepoint.(first(s, 32) * repeat(" ", 32-length(s))))
orderinghash(A::AbstractArray) = orderinghash(first(A))

#########################
# MOUNTED S3 FILESYSTEM #
#########################

function get_s3fs_path(path)
    # Get information about requested object
    s3path = S3Path(path)
    bucket = s3path.bucket
    key = s3path.key
    # bucket = "banyan-cluster-data-myfirstcluster"
    mount = joinpath(homedir(), ".banyan", "mnt", "s3", bucket)

    # Ensure path to mount exists
    if !isdir(mount)
        mkpath(mount)
    end

    # Ensure something is mounted
    if !ismount(mount)
        # TODO: Store buckets from different accounts/IAMs/etc. seperately
        try
            ACCESS_KEY_ID = get_aws_config()[:creds].access_key_id
            SECRET_ACCESS_KEY = get_aws_config()[:creds].secret_key
            HOME = homedir()
            run(`echo $ACCESS_KEY_ID:$SECRET_ACCESS_KEY \> $HOME/.passwd-s3fs\; chmod 600 $HOME/.passwd-s3fs`)
            run(`s3fs $bucket $mount -o url=https://s3.$location.amazonaws.com -o endpoint=$location`)
        catch e
            @error """Failed to mount S3 bucket \"$bucket\" at $mount using s3fs with error: $e. Please ensure s3fs is in PATH or mount manually."""
        end
    end

    # Return local path to object
    joinpath(mount, key)
end
