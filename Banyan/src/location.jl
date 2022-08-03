const LocationParameters = Dict{String,Any}

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::String
    dst_name::String
    src_parameters::LocationParameters
    dst_parameters::LocationParameters
    total_memory_usage::Int64
    sample::Sample
    parameters_invalid::Bool
    sample_invalid::Bool

    # function Location(
    #     src_name::String,
    #     dst_name::String,
    #     src_parameters::Dict{String,<:Any},
    #     dst_parameters::Dict{String,<:Any},
    #     total_memory_usage::Union{Int64,Nothing} = nothing,
    #     sample::Sample = Sample(),
    # )
    #     # NOTE: A file might be None and None if it is simply to be cached on
    #     # disk and then read from
    #     # if src_name == "None" && dst_name == "None"
    #     #     error(
    #     #         "Location must either be usable as a source or as a destination for data",
    #     #     )
    #     # end

    #     new(
    #         src_name,
    #         dst_name,
    #         src_parameters,
    #         dst_parameters,
    #         total_memory_usage,
    #         sample
    #     )
    # end
end

struct LocationPath
    original_path::String
    path::String
    path_hash_uint::UInt
    path_hash::String
    format_name::String
    format_version::String

    function LocationPath(path, format_name, format_version)
        # This function is responsible for "normalizing" the path.
        # If there are multiple path strings that are technically equivalent,
        # this function should map them to the same string.
        path_hash = hash(path)
        new(
            path,
            path,
            path_hash,
            string(path_hash),
            format_name,
            format_version
        )
    end
end

global TABLE_FORMATS = ["csv", "parquet", "arrow"]
z
function get_location_path_with_format(p::String, kwargs...)::LocationPath
    if isempty(p)
        return NO_LOCATION_PATH
    end

    format_name = get(kwargs, :format, "jl")
    is_sample_format_arrow = format_name == "arrow"
    if is_sample_format_arrow
        return LocationPath(p, "arrow", get(kwargs, :format_version, "2"))
    else
        for table_format in TABLE_FORMATS
            if occursin(table_format, p) || format_name == p
                return LocationPath(p, "arrow", "2")
            end
        end
    end
    LocationPath(p, "jl", get_julia_version())
end

function get_sample_path_prefix(lp::LocationPath)
    format_name_sep = !isempty(lp.format_name) ? "_" : ""
    format_version_sep = !isempty(lp.format_version) ? "_" : ""
    lp.path_hash * "_" * lp.format_name * format_name_sep * lp.format_version * format_version_sep
end
get_sample_path(lp::LocationPath, sample_rate::Int64) =
    get_sample_path_prefix(lp) * string(sample_rate)
get_metadata_path(lp::LocationPath) = lp.path_hash

Base.hash(lp::LocationPath) = lp.path_hash_uint

const NO_LOCATION_PATH = LocationPath("", "", "")

get_sampling_config(path="", kwargs...) = get_sampling_config(get_location_path_with_format(path; kwargs...))
function get_sampling_configs()
    global session_sampling_configs
    session_sampling_configs[_get_session_id_no_error()]
end
get_sampling_config(l_path::LocationPath)::SamplingConfig =
    get(get_sampling_configs(), l_path, sampling_configs[NO_LOCATION_PATH])

get_sample_rate(p::String; kwargs...) =
    get_sample_rate(get_location_path_with_format(p; kwargs...))
function get_sample_rate(l_path::LocationPath)
    # Get the desired sample rate
    desired_sample_rate = get_sampling_config(l_path).rate

    # Find a cached sample with a similar sample ratBucket=e
    # TODO: Just have a try/catch here so that if the bucket doesn't exist we just return the default
    # TODO: Make the above code get used in location constructors for getting the desired sample rate
    sc = get_sampling_config(l_path)
    pre = sc.force_new_sample_rate ? get_sample_path(l_path, sc.rate) : get_sample_path_prefix(l_path)
    banyan_samples_objects = try
        S3.list_objects_v2(Bucket="banyan_samples", prefix=pre)["Contents"]
    catch
        return desired_sample_rate
    end
    sample_rate = -1
    for banyan_samples_object in banyan_samples_objects
        object_key = banyan_samples_object["Key"]
        if startswith(object_key, banyan_samples_object_prefix)
            object_sample_rate = parse(Int64, object_key[(findlast("_", object_key).start+1):end])
            object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
            curr_sample_rate_diff = abs(object_sample_rate - sample_rate)
            if sample_rate == -1 || object_sample_rate_diff < curr_sample_rate_diff
                sample_rate = object_sample_rate
            end
        end
    end
    sample_rate != -1 ? sample_rate : desired_sample_rate
end

# function get_location(l_path::LocationPath)
#     sessions_dict = get_sessions_dict()
#     session_id = _get_session_id_no_error()
#     desired_sample_rate = if haskey(sessions_dict, session_id)
#         sampling_configs = sessions_dict[session_id].sampling_configs
#         get(sampling_configs, l_path, sampling_configs[NO_LOCATION_PATH]).
# end

function has_metadata(l_path:: LocationPath)::Bool
    try
        !isempty(S3.list_objects_v2(Bucket="banyan_metadata", prefix=get_metadata_path(l_path))["Contents"])
    catch
        false
    end
end

function has_sample(l_path:: LocationPath)::Bool
    sc = get_sampling_config(l_path)
    pre = sc.force_new_sample_rate ? get_sample_path(l_path, sc.rate) : get_sample_path_prefix(l_path)
    try
        !isempty(S3.list_objects_v2(Bucket="banyan_samples", prefix=pre)["Contents"])
    catch
        false
    end
end