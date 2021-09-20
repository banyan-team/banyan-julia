using Pkg
using SHA
using TimeZones
using Base: AbstractVecOrTuple

timezones_built = false

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
        if is_debug_on()
            @show size
        end
        # TODO: Maybe make this larger
        if size ≤ 128
            0
        else
            size
        end
    end

# NOTE: This function is shared between the client library and the PT library
function indexapply(op, objs...; index::Integer=1)
    lists = [obj for obj in objs if (obj isa AbstractVector || obj isa Tuple)]
    length(lists) > 0 || throw(ArgumentError("Expected at least one tuple as input"))
    index = index isa Colon ? length(first(lists)) : index
    operands = [((obj isa AbstractVector || obj isa Tuple) ? obj[index] : obj) for obj in objs]
    indexres = op(operands...)
    res = first(lists)
    if first(lists) isa Tuple
        res = [res...]
        res[index] = indexres
        Tuple(res)
    else
        res = copy(res)
        res[index] = indexres
        res
    end
end

# converts give time as String to local timezone and returns DateTime
function parse_time(time)
    global timezones_built
    if !timezones_built
        TimeZones.build()
        timezones_built = true
    end
    DateTime(astimezone(ZonedDateTime(time * "0000", "yyyy-mm-dd-HH:MM:SSzzzz"), localzone()))
end

function s3_bucket_arn_to_name(s3_bucket_arn)
    # Get s3 bucket name from arn
    s3_bucket_name = last(split(s3_bucket_arn, ":"))
    if endswith(s3_bucket_name, "/")
        s3_bucket_name = s3_bucket_name[1:end-1]
    elseif endswith(s3_bucket_name, "/*")
        s3_bucket_name = s3_bucket_name[1:end-2]
    elseif endswith(s3_bucket_name, "*")
        s3_bucket_name = s3_bucket_name[1:end-1]
    end
    return s3_bucket_name
end

function s3_bucket_name_to_arn(s3_bucket_name)
    # Get s3 bucket arn from name
    s3_bucket_arn = s3_bucket_name
    if endswith(s3_bucket_arn, "/")
        s3_bucket_arn = s3_bucket_arn[1:end-1]
    elseif endswith(s3_bucket_arn, "/*")
        s3_bucket_arn = s3_bucket_arn[1:end-2]
    elseif endswith(s3_bucket_arn, "*")
        s3_bucket_arn = s3_bucket_arn[1:end-1]
    end
    return s3_bucket_arn
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
    @show banyan_config
    TOML.print(f, banyan_config)
    close(f)
end

if_in_or(key, obj, el = nothing) =
    if key in keys(obj) && !isnothing(obj[key])
        obj[key]
    else
        el
    end

function configure(; kwargs...)
    # This function allows for users to configure their authentication.
    # Authentication details are then saved in
    # `$HOME/.banyan/banyanconfig.toml` so they don't have to be entered in again
    # each time a program using the Banyan client library is run

    # Credentials are checked in the following locations in this order:
    #   1) function arguments, specified in kwargs
    #   2) environment variables
    #   3) `$HOME/.banyan/banyanconfig.toml`

    load_config()
    global banyan_config

    # Load arguments
    kwargs = Dict(kwargs)
    # username = if_in_or(:username, kwargs)
    user_id = if_in_or(:user_id, kwargs)
    api_key = if_in_or(:api_key, kwargs)
    ec2_key_pair_name = if_in_or(:ec2_key_pair_name, kwargs)
    require_ec2_key_pair_name =
        if_in_or(:require_ec2_key_pair_name, kwargs, false)

    # Check environment variables
    if isnothing(user_id) && haskey(ENV, "BANYAN_USER_ID")
        user_id = ENV["BANYAN_USER_ID"]
    end
    if isnothing(api_key) && haskey(ENV, "BANYAN_API_KEY")
        api_key = ENV["BANYAN_API_KEY"]
    end
    # if isnothing(username) && haskey(ENV, "BANYAN_USERNAME")
    #     api_key = ENV["BANYAN_USERNAME"]
    # end

    # Check banyanconfig file
    if isnothing(user_id) && haskey(banyan_config, "banyan") && haskey(banyan_config["banyan"], "user_id")
        user_id = banyan_config["banyan"]["user_id"]
    end
    if isnothing(api_key) && haskey(banyan_config, "banyan") && haskey(banyan_config["banyan"], "api_key")
        api_key = banyan_config["banyan"]["api_key"]
    end
    # if isnothing(username) && haskey(banyan_config, "banyan") && haskey(banyan_config["banyan"], "username")
    #     username = banyan_config["banyan"]["username"]
    # end

    # Initialize
    is_modified = false
    is_valid = true

    # Ensure a configuration has been created or can be created. Otherwise,
    # return nothing
    if isnothing(banyan_config)
        if !isnothing(user_id) && !isnothing(api_key) && !isnothing(username)
            banyan_config = Dict(
                "banyan" =>
                    Dict("username" => username, "user_id" => user_id, "api_key" => api_key),
                "aws" => Dict(),
            )
            is_modified = true
        else
            error("Your username, user ID, and API key must be specified using either keyword arguments, environment variables, or banyanconfig.toml")
        end
    end

    # Check for changes in required
    # if !isnothing(username) &&
    #    (username != banyan_config["banyan"]["username"])
    #     banyan_config["banyan"]["username"] = username
    #     is_modified = true
    # end
    if !isnothing(user_id) &&
        (user_id != banyan_config["banyan"]["user_id"])
         banyan_config["banyan"]["user_id"] = user_id
         is_modified = true
     end
    if !isnothing(api_key) && (api_key != banyan_config["banyan"]["api_key"])
        banyan_config["banyan"]["api_key"] = api_key
        is_modified = true
    end
    # if !isnothing(username) && (username != banyan_config["banyan"]["username"])
    #     banyan_config["banyan"]["username"] = username
    #     is_modified = true
    # end

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
       !(haskey(banyan_config["aws"], "ec2_key_pair_name"))
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
        write_config()
    end

    return banyan_config
end

function get_aws_config()
    global aws_config_in_usage

    # Get AWS configuration
    if isnothing(aws_config_in_usage)
        # Get region according to ENV, then credentials, then config files
        profile = get(ENV, "AWS_DEFAULT_PROFILE", get(ENV, "AWS_DEFAULT_PROFILE", "banyan_nothing"))
        env_region = get(ENV, "AWS_DEFAULT_REGION", "")
        credentialsfile = read(Inifile(), joinpath(homedir(), ".aws", "credentials"))
        configfile = read(Inifile(), joinpath(homedir(), ".aws", "config"))
        credentials_region = _get_ini_value(credentialsfile, profile, "region", default_value="")
        config_region = _get_ini_value(configfile, profile, "region", default_value="")

        # Choose the region that is not default
        region = env_region
        region = isempty(region) ? credentials_region : region
        region = isempty(region) ? config_region : region

        #println(region)

        if isempty(region)
            throw(ErrorException("Could not discover AWS region to use from looking at AWS_PROFILE, AWS_DEFAULT_PROFILE, AWS_DEFAULT_REGION, HOME/.aws/credentials, and HOME/.aws/config"))
        end

	#println(AWSCredentials())
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

is_debug_on() = "JULIA_DEBUG" in keys(ENV)

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
        "create-job2"
    elseif method == :destroy_job
        "destroy-job"
    elseif method == :describe_jobs
        "describe-jobs"
    elseif method == :evaluate
        "evaluate"
    elseif method == :update_cluster
        "update-cluster"
    elseif method == :set_cluster_ready
        "set-cluster-ready"
    end
end

"""
Sends given request with given content
"""
function request_body(url::AbstractString; kwargs...)
    resp = nothing
    body = sprint() do output
        resp = request(url; output=output, kwargs...)
    end
    return resp, body
end

function request_json(url::AbstractString; kwargs...)
    resp, body = request_body(url; kwargs...)
    return resp, JSON.parse(body)
end

function send_request_get_response(method, content::Dict)
    # Prepare request
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]
    content["debug"] = is_debug_on()
    url = string(BANYAN_API_ENDPOINT, method_to_string(method))
    headers = [
        "content-type" => "application/json",
        "Username-APIKey" => "$user_id-$api_key",
    ]
    resp, data = request_json(
	    url, input=IOBuffer(JSON.json(content)), method="POST", headers=headers
    )
    if resp.status == 403
        throw(ErrorException("Please use a valid user ID and API key. Sign into the dashboard to retrieve these credentials."))
    elseif resp.status == 500 || resp.status == 504
        throw(ErrorException(string(data)))
    end
    return data

end

function send_request_get_response_using_http(method, content::Dict)
    # Prepare request
    # content = convert(Dict{Any, Any}, content)
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]
    content["debug"] = is_debug_on()
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
            rethrow()
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
x = [5,6,7,7]
f(x) = x * 2
orderinghash(x::Any) = x # This lets us handle numbers and dates
orderinghash(s::String) = Integer.(codepoint.(first(s, 32) * repeat(" ", 32-length(s))))
orderinghash(A::AbstractArray) = orderinghash(first(A))

#########
# FILES #
#########

function getnormpath(banyanfile_path, p)
    if startswith(p, "file://")
        prefix, suffix = split(banyanfile_path, "://")
        banyanfile_location_path = dirname(suffix)
        @debug banyanfile_location_path
        prefix * "://" * normpath(banyanfile_location_path, last(split(p, "://")))
    else
        p
    end
end

function load_json(path::String)
    if startswith(path, "file://")
        if !isfile(path[8:end])
            error("File $path does not exist")
        end
        JSON.parsefile(path[8:end])
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        # JSON.parsefile(S3Path(path, config=get_aws_config()))
    elseif startswith(path, "http://") || startswith(path, "https://")
	    JSON.parse(String(HTTP.get(path).body))
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

# Loads file into String and returns
function load_file(path::String)
    if startswith(path, "file://")
        if !isfile(path[8:end])
            error("File $path does not exist")
        end
        String(read(open(path[8:end])))
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        String(read(S3Path(path)))
    elseif startswith(path, "http://") || startswith(path, "https://")
        String(HTTP.get(path).body)
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end


####################################
# JULIA VERSION/PACKAGE MANAGEMENT #
####################################

function get_julia_version()
    return string(VERSION)
end

# Returns the directory in which the Project.toml file is located
function get_julia_environment_dir()
    return replace(Pkg.project().path, "Project.toml" => "")
end

# Returns SHA 256 of a string
function get_hash(s)
    return bytes2hex(sha256(s))
end
