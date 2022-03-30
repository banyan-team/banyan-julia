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

total_memory_usage(val)::Int64 =
    begin
        size = Base.summarysize(val)
        # TODO: Maybe make this larger
        if size ≤ 128
            0
        else
            size
        end
    end

# NOTE: This function is shared between the client library and the PT library
function indexapply(op, objs...; index::Int64=1)
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
        # TODO: Add a try-catch here and then if building fails, @warn that we
        # can't convert to local timezone and simply return
        TimeZones.build()
        timezones_built = true
    end
    DateTime(astimezone(ZonedDateTime(time * "0000", "yyyy-mm-dd-HH:MM:SSzzzz"), localzone()))
end

function s3_bucket_arn_to_name(s3_bucket_arn::String)::String
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

function s3_bucket_name_to_arn(s3_bucket_name::String)::String
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

function load_config(banyanconfig_path=nothing)
    global banyan_config

    if isnothing(banyan_config)
        if isnothing(banyanconfig_path)
            banyanconfig_path = joinpath(homedir(), ".banyan", "banyanconfig.toml")
        end
        if isfile(banyanconfig_path)
            banyan_config = TOML.parsefile(banyanconfig_path)
        end
    end
    banyan_config
end

function write_config(banyanconfig_path=nothing)
    global banyan_config

    # Write to banyanconfig.toml
    if isnothing(banyanconfig_path)
        banyanconfig_path = joinpath(homedir(), ".banyan", "banyanconfig.toml")
    end
    mkpath(joinpath(homedir(), ".banyan"))
    f = open(banyanconfig_path, "w")
    TOML.print(f, banyan_config)
    close(f)
end

configure(; user_id=nothing, api_key=nothing, ec2_key_pair_name=nothing, banyanconfig_path=nothing) =
    configure(
        isnothing(user_id) ? "" : user_id,
        isnothing(api_key) ? "" : api_key,
        isnothing(ec2_key_pair_name) ? "" : ec2_key_pair_name,
        isnothing(banyanconfig_path) ? "" : banyanconfig_path
    )
function configure(user_id, api_key, ec2_key_pair_name, banyanconfig_path)
    @nospecialize
    # This function allows for users to configure their authentication.
    # Authentication details are then saved in
    # `$HOME/.banyan/banyanconfig.toml` so they don't have to be entered in again
    # each time a program using the Banyan client library is run

    # Credentials are checked in the following locations in this order:
    #   1) function arguments, specified in kwargs
    #   2) environment variables
    #   3) `$HOME/.banyan/banyanconfig.toml`

    # Load arguments
    # If an argument is optional (e.g., `ec2_key_pair_name`), we default
    # to 0 to indicate that the argument was not specified. This is so that
    # we can differentiate between a user explicitly providing `nothing` as
    # the value for an arg, versus a default.

    # Load config and set the global variable to it
    c = load_config(banyanconfig_path)
    global banyan_config
    banyan_config = c

    # Check environment variables
    if isempty(user_id) && haskey(ENV, "BANYAN_USER_ID")
        user_id = ENV["BANYAN_USER_ID"]
    end
    if isempty(api_key) && haskey(ENV, "BANYAN_API_KEY")
        api_key = ENV["BANYAN_API_KEY"]
    end
    if isempty(ec2_key_pair_name) && haskey(ENV, "BANYAN_EC2_KEY_PAIR_NAME")
        api_key = ENV["BANYAN_EC2_KEY_PAIR_NAME"]
    end

    # Check banyanconfig file
    banyan_config_has_info = !(isempty(banyan_config) || isempty(banyan_config))
    if isempty(user_id) && banyan_config_has_info && haskey(banyan_config, "banyan") && haskey(banyan_config["banyan"], "user_id")
        user_id = banyan_config["banyan"]["user_id"]
    end
    if isempty(api_key) && banyan_config_has_info && haskey(banyan_config, "banyan") && haskey(banyan_config["banyan"], "api_key")
        api_key = banyan_config["banyan"]["api_key"]
    end
    if isempty(ec2_key_pair_name) && banyan_config_has_info && haskey(banyan_config, "aws") && haskey(banyan_config["aws"], "ec2_key_pair_name")
        ec2_key_pair_name = banyan_config["aws"]["ec2_key_pair_name"]
    end

    # Ensure a configuration has been created or can be created. Otherwise,
    # return nothing
    existing_banyan_config = deepcopy(banyan_config)
    if !isempty(user_id) && !isempty(api_key)
        aws_ec2_config = (!isempty(ec2_key_pair_name) && !isempty(ec2_key_pair_name)) ? Dict("ec2_key_pair_name" => ec2_key_pair_name) : Dict()
        banyan_config = Dict(
            "banyan" =>
                Dict("user_id" => user_id, "api_key" => api_key),
            "aws" => aws_ec2_config,
        )
    else
        error("Your user ID and API key must be specified using either keyword arguments, environment variables, or banyanconfig.toml")
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
    if existing_banyan_config != banyan_config
        write_config(banyanconfig_path)
    end

    return banyan_config
end

function get_aws_config()
    global aws_config_in_usage

    # Get AWS configuration
    if isnothing(aws_config_in_usage)
        # Get region according to ENV, then credentials, then config files
        profile = get(ENV, "AWS_DEFAULT_PROFILE", get(ENV, "AWS_DEFAULT_PROFILE", "default"))
        region::String = get(ENV, "AWS_DEFAULT_REGION", "")
        if region == ""
            try
                configfile = read(Inifile(), joinpath(homedir(), ".aws", "config"))
                @show _get_ini_value(configfile, profile, "region", default_value="")
                region = convert(String, _get_ini_value(configfile, profile, "region", default_value=""))::String
            catch
            end
        end
        if region == ""
            try
                credentialsfile = read(Inifile(), joinpath(homedir(), ".aws", "credentials"))
                region = convert(String, _get_ini_value(credentialsfile, profile, "region", default_value=""))::String
            catch
            end
        end

        if region == ""
            throw(ErrorException("Could not discover AWS region to use from looking at AWS_PROFILE, AWS_DEFAULT_PROFILE, AWS_DEFAULT_REGION, HOME/.aws/credentials, and HOME/.aws/config"))
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

get_aws_config_region() = get_aws_config()[:region]::String

#########################
# ENVIRONMENT VARIABLES #
#########################

is_debug_on() = get(ENV, "JULIA_DEBUG", "") == "Banyan"

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

method_to_string(method::Symbol)::String = begin
    if method == :create_cluster
        "create-cluster"
    elseif method == :destroy_cluster
        "destroy-cluster"
    elseif method == :describe_clusters
        "describe-clusters"
    elseif method == :start_session
        "start-session"
    elseif method == :end_session
        "end-session"
    elseif method == :describe_sessions
        "describe-sessions"
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
function request_body(url::String; kwargs...)
    global downloader
    resp = nothing
    body = sprint() do output
        resp = request(url; output=output, throw=false, downloader=downloader, kwargs...)
    end
    return resp, body
end

function request_json(url::String; kwargs...)
    resp, body = request_body(url; kwargs...)
    return resp, JSON.parse(body)
end

# Sends an HTTP request to the Banyan API and returns the
# parsed response. Sends the provided content as the body of
# the message and additionally adds the User ID and API Key,
# which are required on all requests for authentication. Additionally,
# a debug flag is sent. An exception is thrown is the HTTP
# requests returns  a 403, 500, or 504 HTTP error cdode. If the
# request times out, a warning message is printed out and `nothing`
# is returned.
# It is up to the caller to handle the case where the HTTP request
# times out and `nothing` is returned.
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
    # Look for BANYAN_GITHUB_TOKEN environment variable if we are starting a session
    # Should be in the form https://<username>:<private_access_token>@github.com
    # Also, look for BANYAN_SSH_KEY_PATH environment variable if we are starting as session.
    # This is the path to the private SSH key on the cluster that the user should have added.
    if haskey(ENV, "BANYAN_GITHUB_TOKEN")
        push!(headers, "banyan-github-token" => ENV["BANYAN_GITHUB_TOKEN"])
        # Cache
        configuration["banyan"]["banyan_github_token"] = ENV["BANYAN_GITHUB_TOKEN"]
        write_config()
    elseif haskey(configuration["banyan"], "banyan_github_token")
        push!(headers, "banyan-github-token" => configuration["banyan"]["banyan_github_token"])
    end
    if haskey(ENV, "BANYAN_SSH_KEY_PATH")
        push!(headers, "banyan-ssh-key-path" => ENV["BANYAN_SSH_KEY_PATH"])
        configuration["banyan"]["banyan_ssh_key_path"] = ENV["BANYAN_SSH_KEY_PATH"]
        write_config()
    elseif haskey(configuration["banyan"], "banyan_ssh_key_path")
        push!(headers, "banyan-ssh-key-path" => configuration["banyan"]["banyan_ssh_key_path"])
    end
    resp, data = request_json(
	    url, input=IOBuffer(JSON.json(content)), method="POST", headers=headers
    )
    if resp.status == 403
        error("Please use a valid user ID and API key. Sign into the dashboard to retrieve these credentials.")
    elseif resp.status == 504
        # HTTP request timed out, for example
        if isa(data, Dict) && haskey(data, "message")
            data = data["message"]
        end
        @error data
        return nothing
    elseif resp.status == 500 || resp.status == 504
        error(data)
    elseif resp.status == 502
        error("Sorry there has been an error. Please contact support.")
    end
    return data

end

# function send_request_get_response_using_http(method, content::Dict)
#     # Prepare request
#     # content = convert(Dict{Any, Any}, content)
#     configuration = load_config()
#     user_id = configuration["banyan"]["user_id"]
#     api_key = configuration["banyan"]["api_key"]
#     content["debug"] = is_debug_on()
#     url = string(BANYAN_API_ENDPOINT, method_to_string(method))
#     headers = (
#         ("content-type", "application/json"),
#         ("Username-APIKey", "$user_id-$api_key"),
#     )

#     # Post and return response
#     try
#         response = HTTP.post(url, headers, JSON.json(content))
#         body = String(response.body)
#         return JSON.parse(body)
#     catch e
#         if e isa HTTP.ExceptionRequest.StatusError
#             if e.response.status == 403
#                 throw(
#                     ErrorException(
#                         "Please set a valid api_key. Sign in to the dashboard to retrieve your api key.",
#                     ),
#                 )
#             end
#             if e.response.status != 504
#                 throw(ErrorException(String(take!(IOBuffer(e.response.body)))))
#             end
#             rethrow()
#         else
#             rethrow()
#         end
#     end
# end

#########
# FILES #
#########

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
	    JSON.parse(request_body(path)[2])
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

function load_toml(path::String)
    if startswith(path, "file://")
        if !isfile(path[8:end])
            error("File $path does not exist")
        end
        TOML.parsefile(path[8:end])
    elseif startswith(path, "s3://")
        error("S3 path not currently supported")
        # JSON.parsefile(S3Path(path, config=get_aws_config()))
    elseif startswith(path, "http://") || startswith(path, "https://")
	    TOML.parse(request_body(path)[2])
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

function load_json(paths::Vector{String})
    # Each file should have merges, splits, and casts. So we need to take those
    # and merge them.
    mergewith(merge, [load_json(p) for p in paths]...)
end

load_toml(paths::Vector{String}) = mergewith(merge, [load_toml(p) for p in paths]...)

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
        request_body(path)[2]
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

function get_loaded_packages()
    # for m in names(Main, imported=true)
    #     @show try Main.eval(m) catch nothing end isa Module && !(m in [:Main, :Base, :Core, :InteractiveUtils, :IJulia])
    # end
    modules = map(
        m -> string(m),
        filter(
            m -> try Main.eval(m) catch nothing end isa Module && !(m in [:Main, :Base, :Core, :InteractiveUtils, :IJulia, :VSCodeServer]),
            names(Main, imported=true)
        )
    )
end

# Returns the directory in which the Project.toml file is located
function get_julia_environment_dir()
    return replace(Pkg.project().path, "Project.toml" => "")
end

# Returns SHA 256 of a string
function get_hash(s)
    return bytes2hex(sha256(s))
end

function format_bytes(bytes, decimals = 2)
    bytes == 0 && return "0 Bytes"
    k = 1024
    dm = decimals < 0 ? 0 : decimals
    sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    i = Base.convert(Int, floor(log(bytes) / log(k)))
    return string(round((bytes / ^(k, i)), digits = dm)) * " " * sizes[i+1]
end

byte_sizes = Dict(
    "kB" => 10 ^ 3,
    "MB" => 10 ^ 6,
    "GB" => 10 ^ 9,
    "TB" => 10 ^ 12,
    "PB" => 10 ^ 15,
    "KiB" => 2 ^ 10,
    "MiB" => 2 ^ 20,
    "GiB" => 2 ^ 30,
    "TiB" => 2 ^ 40,
    "PiB" => 2 ^ 50,
    "B" => 1,
    "" => 1,
)

byte_sizes = Dict(lowercase(k) => v for (k, v) in byte_sizes)
merge!(byte_sizes, Dict(string(k[1]) => v for (k, v) in byte_sizes if !isempty(k) && !occursin("i", k)))
merge!(byte_sizes, Dict(k[1:end-1] => v for (k, v) in byte_sizes if !isempty(k) && occursin("i", k)))

parse_bytes(r::Real)::Float64 = convert(Float64, r)

function parse_bytes(s::String)::Float64
    s = replace(s, " " => "")
    if !any([isdigit(char) for char in s])
        s = "1" * s
    end

    index = -1
    for i in length(s):-1:0
        if !isletter(s[i])
            index = i + 1
            break
        end
    end

    prefix = s[1:index-1]
    suffix = s[index:end]

    n = -1
    try
        n = parse(Float64, prefix)
    catch
        throw(ArgumentError("Could not interpret '$prefix' as a number"))
    end

    multiplier = -1
    try
        multiplier = byte_sizes[lowercase(suffix)]
    catch
        throw(ArgumentError("Could not interpret '$suffix' as a byte unit"))
    end

    result = n * multiplier
    result
end

function get_branch_name()
    prepo = LibGit2.GitRepo(realpath(joinpath(@__DIR__, "../..")))
    phead = LibGit2.head(prepo)
    branchname = LibGit2.shortname(phead)
    branchname
end

struct Empty end
const EMPTY = Empty()
nonemptytype(::Type{T}) where {T} = typesplit(T, Empty)
disallowempty(x::AbstractArray{T}) where {T} = convert(AbstractArray{nonemptytype(T)}, x)
function empty_handler(op)
    (a, b) -> if a isa Empty
        b
    elseif b isa Empty
        a
    else
        op(a, b)
    end
end

reduce_sizes_and_eltypes(a, b) =
    begin
        if a[1] isa Empty
            b
        elseif b[1] isa Empty
            a
        else
            (a[1] .+ b[1], a[2])
        end
    end