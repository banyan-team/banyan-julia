using Pkg
using SHA
using TimeZones
using Base: AbstractVecOrTuple

timezones_built = false

const EMPTY_DICT = Dict{String,Any}()

##############
# CONVERSION #
##############

# NOTE: `jl` referes to a subset of Julia that can be serialized to or
# deserialized from JSON with ease

jl_to_json(j) = JSON.json(j)

json_to_jl(j) = JSON.parse(j)

key_to_jl(key) = reinterpret(UInt8, hash(string(key))) |> String
axis_to_jl(axis) = reinterpret(UInt8, hash(string(key))) |> String

sample_memory_usage(val::Any)::Int64 =
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
function indexapply(op::Function, obj::NTuple{N,Int64}, index::Int64) where {N}
    res = Base.collect(obj)
    res[index] = op(obj[index])
    Tuple(res)
end
function indexapply(op::Function, obj_a::NTuple{N,Int64}, obj_b::NTuple{N,Int64}, index::Int64) where {N}
    res = Base.collect(obj_a)
    res[index] = op(obj_a[index], obj_b[index])
    Tuple(res)
end
function indexapply(val::Int64, obj::NTuple{N,Int64}, index::Int64) where {N}
    res = Base.collect(obj)
    res[index] = val
    Tuple(res)
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
    s3_bucket_name = split(s3_bucket_arn, ":")[end]
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
    "arn:aws:s3:::$s3_bucket_name*"
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

@nospecialize

function load_config(banyanconfig_path::String)
    global banyan_config

    if isnothing(banyan_config)
        if isfile(banyanconfig_path)
            banyan_config = TOML.parsefile(banyanconfig_path)
        end
    end
    banyan_config
end

function write_config(banyanconfig_path::String)
    global banyan_config

    # Write to banyanconfig.toml
    mkpath(joinpath(homedir(), ".banyan"))
    f = open(banyanconfig_path, "w")
    TOML.print(f, banyan_config)
    close(f)
end

get_banyanconfig_path()::String = joinpath(homedir(), ".banyan", "banyanconfig.toml")

configure(; user_id=nothing, api_key=nothing, ec2_key_pair_name=nothing, banyanconfig_path=nothing, kwargs...) =
    configure(
        isnothing(user_id) ? "" : user_id,
        isnothing(api_key) ? "" : api_key,
        isnothing(ec2_key_pair_name) ? "" : ec2_key_pair_name,
        isnothing(banyanconfig_path) ? get_banyanconfig_path() : banyanconfig_path
    )

function configure(user_id, api_key, ec2_key_pair_name, banyanconfig_path)
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
    banyan_config_has_info = !isnothing(banyan_config) && !isempty(banyan_config)
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
        aws_ec2_config = (!isempty(ec2_key_pair_name) && !isempty(ec2_key_pair_name)) ? Dict{String,String}("ec2_key_pair_name" => ec2_key_pair_name) : Dict{String,String}()
        banyan_config = Dict{String,Any}(
            "banyan" =>
                Dict{String,String}("user_id" => user_id, "api_key" => api_key),
            "aws" => aws_ec2_config,
        )
    else
        error("Your user ID and API key must be specified using either keyword arguments, environment variables, or banyanconfig.toml")
    end

    # Update config file if it was modified
    if existing_banyan_config != banyan_config
        write_config(banyanconfig_path)
    end

    return banyan_config
end

# Getting organization IDs

organization_ids = Dict{String,String}()
function get_organization_id()
    global organization_ids
    global sessions
    session_id = _get_session_id_no_error()
    if haskey(sessions, session_id)
        sessions[session_id].organization_id
    else
        user_id = configure()["banyan"]["user_id"]
        if haskey(organization_ids, user_id)
            organization_ids[user_id]
        else
            organization_id = send_request_get_response(:describe_users, Dict{String,Any}())["organization_id"]
            organization_ids[user_id] = organization_id
            organization_id
        end
    end
end

@specialize

get_aws_config_region() = global_aws_config().region

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
    elseif method == :describe_users
        "describe-users"
    elseif method == :create_process
        "create-process"
    elseif method == :destroy_process
        "destroy-process"
    elseif method == :describe_processes
        "describe-processes"
    elseif method == :run_process
        "run_process"
    elseif method == :stop_process
        "stop_process"


    end
end

"""
Sends given request with given content
"""
function request_body(url::String; @nospecialize(kwargs...))
    global downloader
    s = IOBuffer(sizehint=0)
    output = IOContext(s)
    resp = request(url; output=output, throw=false, downloader=downloader, kwargs...)
    body::String = String(resize!(s.data, s.size))
    return resp, body
end

function request_json(url::String; @nospecialize(kwargs...))
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
    configuration = load_config(get_banyanconfig_path())
    user_id = configuration["banyan"]["user_id"]
    @show user_id
    api_key = configuration["banyan"]["api_key"]
    content["debug"] = is_debug_on()
    url = string(BANYAN_API_ENDPOINT, method_to_string(method))
    headers = [
        "content-type" => "application/json",
        "Username-APIKey" => "$user_id-$api_key",
    ]
    @show headers
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
	    url; input=IOBuffer(JSON.json(content)), method="POST", headers=headers
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
        error("Sorry, an error has occurred. Please contact us at support@banyancomputing.com or use the Banyan Users Slack for assistance.")
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
        # TODO: Maybe support with
        # `JSON.parsefile(S3Path(path, config=global_aws_config()))` and also down
        # in `load_toml`
    elseif startswith(path, "http://") || startswith(path, "https://")
	    JSON.parse(request_body(path)[2])
    else
        error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
    end
end

# function load_toml(path::String)
#     res = if startswith(path, "file://")
#         if !isfile(path[8:end])
#             error("File $path does not exist")
#         end
#         TOML.parsefile(path[8:end])
#     elseif startswith(path, "s3://")
#         error("S3 path not currently supported")
#         # JSON.parsefile(S3Path(path, config=global_aws_config()))
#     elseif startswith(path, "http://") || startswith(path, "https://")
# 	    TOML.parse(request_body(path)[2])
#     else
#         error("Path $path must start with \"file://\", \"s3://\", or \"http(s)://\"")
#     end
#     res
# end

function load_json(paths::Vector{String})
    # Each file should have merges, splits, and casts. So we need to take those
    # and merge them.
    mergewith(merge, map(load_json, paths)...)
end

# function load_toml(paths::Vector{String})
#     npaths = length(paths)
#     loaded_dir = mktempdir()
#     Threads.@threads for i = 1:npaths
#         Downloads.download(paths[i], joinpath(loaded_dir, "part" * string(i)))
#     end
#     loaded = Vector{String}(undef, npaths)
#     for i = 1:npaths
#         loaded[i] = "file://" * joinpath(loaded_dir, "part$i")
#     end
#     res = mergewith(merge, map(load_toml, loaded)...)
#     rm(loaded_dir, recursive=true)
#     res
# end

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

byte_sizes = Dict{String,Int64}(
    "kB" => Int64(10 ^ 3),
    "MB" => Int64(10 ^ 6),
    "GB" => Int64(10 ^ 9),
    "TB" => Int64(10 ^ 12),
    "PB" => Int64(10 ^ 15),
    "KiB" => Int64(2 ^ 10),
    "MiB" => Int64(2 ^ 20),
    "GiB" => Int64(2 ^ 30),
    "TiB" => Int64(2 ^ 40),
    "PiB" => Int64(2 ^ 50),
    "B" => Int64(1),
    "" => Int64(1),
)

byte_sizes = Dict{String,Int64}(lowercase(k) => v for (k, v) in byte_sizes)
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

function get_branch_name()::String
    prepo = LibGit2.GitRepo(realpath(joinpath(@__DIR__, "../..")))
    phead = LibGit2.head(prepo)
    branchname = LibGit2.shortname(phead)
    branchname
end

struct Empty end
const EMPTY = Empty()
nonemptytype(::Type{T}) where {T} = Base.typesplit(T, Empty)
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

isnotempty(x) = !isempty(x)

fsync_file(p) =
    open(p) do f
        # TODO: Maybe use MPI I/O method for fsync instead
        ccall(:fsync, Cint, (Cint,), fd(f))
    end

deserialize_retry = retry(deserialize; delays=Base.ExponentialBackOff(; n=5))

exponential_backoff_1s =
    Base.ExponentialBackOff(; n=5, first_delay=0.1, factor=1.6)

# ```
# julia> for f in Base.ExponentialBackOff(; n=5, first_delay=0.1, factor=1.5)
#        println(f)
#        sleep(f)
#        end
# 0.1
# 0.13579474148420326
# 0.20068919503553564
# 0.29422854986603664
# 0.4414150248213825
# ````

invert(my_dict::AbstractDict) = Dict(value => key for (key, value) in my_dict)

TYPE_TO_STR =
    Dict{DataType,String}(
        Int8 => "int8",
        Int16 => "int16",
        Int32 => "int32",
        Int64 => "int64",
        Int128 => "int128",
        Float16 => "float16",
        Float32 => "float32",
        Float64 => "float64",
        String => "str",
        Bool => "bool",
    )

STR_TO_TYPE = invert(TYPE_TO_STR)

function type_to_str(ty::DataType)::String
    global TYPE_TO_STR
    if haskey(TYPE_TO_STR, ty)
        TYPE_TO_STR[ty]
    else
        "lang_jl_" * to_jl_string(ty)
    end
end

function type_from_str(s::String)
    if startswith(s, "lang_")
        if startswith(s, "lang_jl_")
            from_jl_string(s[9:end])
        else
            error("Cannot parse type $s from non-Julia language")
        end
    elseif haskey(STR_TO_TYPE, s)
        STR_TO_TYPE[s]
    else
        error("Type not supported. You may need to update to the latest version of Banyan or declare the data/sample/metadata you are accessing invalid.")
    end
end

size_to_str(sz) = join(map(string, sz), ",")
size_from_str(s) =
    let sz_strs = split(s, ",")
        res = Vector{Int64}(undef, length(sz_strs))
        for (i, sz_str) in enumerate(sz_strs)
            res[i] = parse(Int64, sz_str)
        end
        Tuple(res)
    end
    
function isdir_no_error(p)
    try
        isdir(p)
    catch e
        if is_debug_on()
            print("Failed to check isdir because of e=$e")
        end
        false
    end
end
function path_as_dir(p)
    p_sep = p.separator
    endswith(string(p), p_sep) ? p : (p * p_sep)
end
function readdir_no_error(p)
    try
        readdir(path_as_dir(p))
    catch e
        if is_debug_on()
            print("Failed to readdir of p=$p because of e=$e")
        end
        String[]
    end
end

struct AWSExceptionInfo
    is_aws::Bool
    unmodified_since::Bool
    not_found::Bool

    function AWSExceptionInfo(e)
        is_aws = e isa AWSException && e.cause isa AWS.HTTP.ExceptionRequest.StatusError
        new(is_aws, is_aws && e.cause.status == 304, is_aws && e.cause.status == 404)
    end
end