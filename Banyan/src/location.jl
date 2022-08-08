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
    metadata_invalid::Bool
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

    LocationPath(path) = LocationPath(path, "jl", get_julia_version())``
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
banyan_samples_bucket_name() = "banyan-samples-$(get_organization_id())"
banyan_metadata_bucket_name() = "banyan-metadata-$(get_organization_id())"

Base.hash(lp::LocationPath) = lp.path_hash_uint

const NO_LOCATION_PATH = LocationPath("", "", "")

get_sampling_config(path="", kwargs...) = get_sampling_config(get_location_path_with_format(path; kwargs...))
function get_sampling_configs()
    global session_sampling_configs
    session_sampling_configs[_get_session_id_no_error()]
end
get_sampling_config(l_path::LocationPath)::SamplingConfig =
    let scs = get_sampling_configs()
        get(scs, l_path, scs[NO_LOCATION_PATH])
    end

get_sample_rate(p::String=""; kwargs...) =
    get_sample_rate(get_location_path_with_format(p; kwargs...))
parse_sample_rate(object_key) =
    parse(Int64, object_key[(findlast("_", object_key).start+1):end])
function get_sample_rate(l_path::LocationPath)
    # Get the desired sample rate
    desired_sample_rate = get_sampling_config(l_path).rate

    # If we just want the default sample rate or if a new sample rate is being
    # forced, then just return that.
    if isempty(l_path.path)
        return desired_sample_rate
    end
    sc = get_sampling_config(l_path)
    if sc.force_new_sample_rate
        return desired_sample_rate
    end

    # Find a cached sample with a similar sample rate
    pre = get_sample_path_prefix(l_path)
    banyan_samples_objects = try
        res = S3.list_objects_v2(banyan_samples_bucket_name(), Dict("prefix" => pre))["Contents"]
        res isa Base.Vector ? res : [res]
    catch
        return desired_sample_rate
    end
    sample_rate = -1
    for banyan_samples_object in banyan_samples_objects
        object_key = banyan_samples_object["Key"]
        if startswith(object_key, pre)
            object_sample_rate = parse_sample_rate(object_key)
            object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
            curr_sample_rate_diff = abs(object_sample_rate - sample_rate)
            if sample_rate == -1 || object_sample_rate_diff < curr_sample_rate_diff
                sample_rate = object_sample_rate
            end
        end
    end
    sample_rate != -1 ? sample_rate : desired_sample_rate
end

function has_metadata(l_path:: LocationPath)::Bool
    try
        !isempty(S3.list_objects_v2(banyan_metadata_bucket_name(), Dict("prefix" => get_metadata_path(l_path)))["Contents"])
    catch
        false
    end
end

function has_sample(l_path:: LocationPath)::Bool
    sc = get_sampling_config(l_path)
    pre = sc.force_new_sample_rate ? get_sample_path(l_path, sc.rate) : get_sample_path_prefix(l_path)
    try
        !isempty(S3.list_objects_v2(banyan_samples_bucket_name(), Dict("prefix" => pre))["Contents"])
    catch
        false
    end
end

twodigit(i::Int64) = i < 10 ? ("0" * string(i)) : string(i)

get_src_params_dict(d::Union{Nothing,Base.ImmutableDict{String, String}}) =
    isnothing(d) ? Dict{String,String}() : Dict{String,String}(d)

get_src_params_dict_from_arrow(p) = Arrow.Table(p) |> Arrow.getmetadata |> get_src_params_dict

struct AWSExceptionInfo
    is_aws::Bool
    unmodified_since::Bool
    not_found::Bool

    function AWSExceptionInfo(e)
        is_aws = e isa AWSException && e.cause isa AWS.HTTP.ExceptionRequest.StatusError
        new(is_aws, is_aws && e.cause.status == 304, is_aws && e.cause.status == 404)
    end
end

function get_location_source(lp::LocationPath)::Tuple{Location,String,String}
    # This checks local cache and S3 cache for sample and metadata files.
    # It then returns a Location object (with a null sample) and the local file names
    # to read/write the metadata and sample from/to.

    # Load in metadata
    metadata_path = get_metadata_path(lp)
    metadata_local_path = joinpath(homedir(), ".banyan", "metadata", metadata_path)
    metadata_s3_path = "/$(banyan_metadata_bucket_name())/$metadata_path"
    src_params_not_stored_locally = false
    src_params::Dict{String, String} = if isfile(metadata_local_path)
        lm = Dates.unix2datetime(mtime(metadata_local_path))
        if_modified_since_string =
            "$(dayabbr(lm)), $(twodigit(day(lm))) $(monthabbr(lm)) $(year(lm)) $(twodigit(hour(lm))):$(twodigit(minute(lm))):$(twodigit(second(lm))) GMT"
        try
            d = get_src_params_dict_from_arrow(s3("GET", metadata_s3_path, Dict("headers" => Dict("If-Modified-Since" => if_modified_since_string))))
            src_params_not_stored_locally = true
            d
        catch e
            if is_debug_on()
                show(e)
            end
            ei = AWSExceptionInfo(e)
            if ei.not_found
                Dict{String, String}()
            elseif ei.unmodified_since
                get_src_params_dict_from_arrow(metadata_local_path)
            else
                @warn "Assumming locally stored metadata is invalid because of following error in accessing the metadata copy in the cloud"
                show(e)
                Dict{String, String}()
            end
        end
    else
        try
            d = get_src_params_dict_from_arrow(s3("GET", metadata_s3_path))
            src_params_not_stored_locally = true
            d
        catch e
            if is_debug_on()
                show(e)
            end
            if !AWSExceptionInfo(e).not_found
                @warn "Assumming metadata isn't copied in the cloud because of following error in attempted access"
                show(e)
            end
            Dict{String, String}()
        end
    end
    # Store metadata locally
    if src_params_not_stored_locally && !isempty(d)
        Arrow.write(metadata_local_path, Arrow.Table(); metadata=src_params)
    end

    # Load in sample

    sc = get_sampling_config()
    force_new_sample_rate = sc.force_new_sample_rate
    desired_sample_rate = sc.rate
    sample_path_prefix = get_sample_path_prefix(lp)

    # Find local samples
    found_local_samples = Tuple{String,Int64}[]
    found_local_sample_rate_diffs = Int64[]
    samples_local_dir = joinpath(homedir(), ".banyan", "samples")
    for local_sample_path in readdir(samples_local_dir, join=true)
        if startswith(local_sample_path, sample_path_prefix)
            local_sample_rate = parse_sample_rate(object_key)
            diff_sample_rate = abs(local_sample_rate - desired_sample_rate)
            if !force_new_sample_rate || sample_rate_diff == 0
                push!(found_local_samples, (local_sample_path, local_sample_rate))
                push!(found_local_sample_rate_diffs, diff_sample_rate)
            end
        end
    end

    # Sort in descending suitability (the most suitable sample is the one with sample
    # rate closest to the desired sample rate)
    found_local_samples = found_local_samples[sortperm(found_local_sample_rate_diffs)]

    # Find a local sample that is up-to-date
    final_local_sample_path = ""
    for (sample_local_path, sample_rate) in found_local_samples
        lm = Dates.unix2datetime(mtime(sample_local_path))
        if_modified_since_string =
            "$(dayabbr(lm)), $(twodigit(day(lm))) $(monthabbr(lm)) $(year(lm)) $(twodigit(hour(lm))):$(twodigit(minute(lm))):$(twodigit(second(lm))) GMT"
        sample_s3_path = "/$(banyan_samples_bucket_name())/$sample_path_prefix$sample_rate"
        try
            blob = s3("GET", sample_s3_path, Dict("headers" => Dict("If-Modified-Since" => if_modified_since_string)))
            write(sample_local_path, blob)  # This overwrites the existing file
            final_local_sample_path = sample_local_path
            break
        catch e
            if is_debug_on()
                show(e)
            end
            ei = AWSExceptionInfo(e)
            if ei.not_found
                @warn "Assumming locally stored metadata is invalid because it is not backed up to the cloud"
            elseif ei.unmodified_since
                final_local_sample_path = sample_local_path
                break
            else
                @warn "Assumming locally stored metadata is invalid because of following error in accessing the metadata copy in the cloud"
                show(e)
            end
        end
    end

    # If no such sample is found, search the S3 bucket
    banyan_samples_objects = try
        res = S3.list_objects_v2(banyan_samples_bucket_name(), Dict("prefix" => sample_path_prefix))["Contents"]
        res isa Base.Vector ? res : [res]
    catch e
        if is_debug_on()
            show(e)
        end
        []
    end
    banyan_samples_object_sample_rate = -1
    for banyan_samples_object in banyan_samples_objects
        object_key = banyan_samples_object["Key"]
        if startswith(object_key, banyan_samples_object_prefix)
            object_sample_rate = parse_sample_rate(object_key)
            object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
            curr_sample_rate_diff = abs(object_sample_rate - sample_rate)
            if sample_rate == -1 || object_sample_rate_diff < curr_sample_rate_diff
                banyan_samples_object_sample_rate = object_sample_rate
            end
        end
    end
    if banyan_samples_object_sample_rate != -1
        sample_path_suffix = "$sample_path_prefix$banyan_samples_object_sample_rate"
        blob = s3("GET", "/$(banyan_samples_bucket_name())/$sample_path_suffix")
        final_local_sample_path = joinpath(samples_local_dir, sample_path_suffix)
        write(final_local_sample_path, blob)
    end
    
    # Construct and return LocationSource
    res_location = LocationSource(
        get(src_params, "name", "Remote"),
        src_params,
        parse(Int64, get(src_params, "total_memory_usage", "0")),
        NOTHING_SAMPLE
    )
    res_location.metadata_invalid = isempty(src_params)
    res_location.sample_invalid = isempty(final_local_sample_path)
    (
        res_location,
        metaata_local_path,
        isempty(final_local_sample_path) ? final_local_sample_path : "sample_path_prefix$desired_sample_rate"
    )
end