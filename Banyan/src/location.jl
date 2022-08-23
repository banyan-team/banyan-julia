const LocationParameters = Dict{String,Any}

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::String
    dst_name::String
    src_parameters::LocationParameters
    dst_parameters::LocationParameters
    sample_memory_usage::Int64
    sample::Sample
    metadata_invalid::Bool
    sample_invalid::Bool
end

LOCATION_PATH_KWARG_NAMES = ["add_channelview"]

struct LocationPath
    original_path::String
    path::String
    path_hash_uint::UInt
    path_hash::String
    format_name::String
    format_version::String

    function LocationPath(path::Any, format_name::String, format_version::String; kwargs...)
        LocationPath("lang_jl_$(hash(path))", format_name, format_version; kwargs...)
    end
    function LocationPath(path::String, format_name::String, format_version::String; kwargs...)
        # This function is responsible for "normalizing" the path.
        # If there are multiple path strings that are technically equivalent,
        # this function should map them to the same string.

        # Add the kwargs to the path
        path_res = deepcopy(path)
        for (kwarg_name, kwarg_value) in kwargs
            if kwarg_name in LOCATION_PATH_KWARG_NAMES
                path_res *= "_$kwarg_name=$kwarg_value"
            end
        end

        # Return the LocationPath
        path_hash = hash(path_res)
        new(
            path_res,
            path_res,
            path_hash,
            string(path_hash),
            format_name,
            format_version
        )
    end
    
    LocationPath(p; kwargs...) = LocationPath("lang_jl_$(hash(path))"; kwargs...)
    function LocationPath(p::String; kwargs...)::LocationPath
        if isempty(p)
            return NO_LOCATION_PATH
        end
    
        format_name = get(kwargs, :format, "jl")
        is_sample_format_arrow = format_name == "arrow"
        if is_sample_format_arrow
            return LocationPath(p, "arrow", get(kwargs, :format_version, "2"); kwargs...)
        else
            for table_format in TABLE_FORMATS
                if occursin(table_format, p) || format_name == p
                    return LocationPath(p, "arrow", "2"; kwargs...)
                end
            end
        end
        LocationPath(p, "jl", get_julia_version(); kwargs...)
    end

    # TODO: Maybe make 
end

# Functions with `LocationPath`s`

global TABLE_FORMATS = ["csv", "parquet", "arrow"]

function get_sample_path_prefix(lp::LocationPath)
    format_name_sep = !isempty(lp.format_name) ? "_" : ""
    lp.path_hash * "_" * lp.format_name * format_name_sep * lp.format_version
end
get_metadata_path(lp::LocationPath) = lp.path_hash
banyan_samples_bucket_name() = "banyan-samples-$(get_organization_id())"
banyan_metadata_bucket_name() = "banyan-metadata-$(get_organization_id())"

Base.hash(lp::LocationPath) = lp.path_hash_uint

const NO_LOCATION_PATH = LocationPath("", "", "")

# Sample config management

const DEFAULT_SAMPLING_CONFIG = SamplingConfig(1024, false, parse_bytes("32 MB"), false, true)
session_sampling_configs = Dict{SessionId,Dict{LocationPath,SamplingConfig}}("" => Dict(NO_LOCATION_PATH => DEFAULT_SAMPLING_CONFIG))

function set_sampling_configs(d::Dict{LocationPath,SamplingConfig})
    global session_sampling_configs
    session_sampling_configs[_get_session_id_no_error()] = d
end

get_sampling_config(path=""; kwargs...) = get_sampling_config(LocationPath(path; kwargs...))
function get_sampling_configs()
    global session_sampling_configs
    session_sampling_configs[_get_session_id_no_error()]
end
get_sampling_config(l_path::LocationPath)::SamplingConfig =
    let scs = get_sampling_configs()
        get(scs, l_path, scs[NO_LOCATION_PATH])
    end

# Getting sample rate

get_sample_rate(p=""; kwargs...) =
    get_sample_rate(LocationPath(p; kwargs...))
function parse_sample_rate(object_key)
    parse(Int64, last(splitpath(object_key)))
end
function get_sample_rate(l_path::LocationPath)
    sc = get_sampling_config(l_path)

    # Get the desired sample rate
    desired_sample_rate = sc.rate

    # If we just want the default sample rate or if a new sample rate is being
    # forced, then just return that.
    if isempty(l_path.path)
        return desired_sample_rate
    end
    if sc.force_new_sample_rate
        return desired_sample_rate
    end

    # Find a cached sample with a similar sample rate
    banyan_samples_bucket = S3Path("s3://$(banyan_samples_bucket_name())")
    banyan_samples_object_dir = joinpath(banyan_samples_bucket, get_sample_path_prefix(l_path))
    sample_rate = -1
    for object_key in readdir_no_error(banyan_samples_object_dir)
        object_sample_rate = parse(Int64, object_key)
        object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
        curr_sample_rate_diff = abs(sample_rate - desired_sample_rate)
        if sample_rate == -1 || object_sample_rate_diff < curr_sample_rate_diff
            sample_rate = object_sample_rate
        end
    end
    sample_rate != -1 ? sample_rate : desired_sample_rate
end

# Checking for having metadata, samples

has_metadata(p=""; kwargs...) =
    has_metadata(LocationPath(p; kwargs...))
function has_metadata(l_path:: LocationPath)::Bool
    isfile(S3Path("s3://$(banyan_metadata_bucket_name())/$(get_metadata_path(l_path))"))
end

has_sample(p=""; kwargs...) =
    has_sample(LocationPath(p; kwargs...))
function has_sample(l_path:: LocationPath)::Bool
    sc = get_sampling_config(l_path)
    banyan_sample_dir = S3Path("s3://$(banyan_samples_bucket_name())/$(get_sample_path_prefix(l_path))")
    if sc.force_new_sample_rate
        isfile(joinpath(banyan_sample_dir, string(sc.rate)))
    else
        !isempty(readdir_no_error(banyan_sample_dir))
    end
end

# Helper function for getting `Location` for location constructors

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

function get_metadata_local_path()
    p = joinpath(homedir(), ".banyan", "metadata")
    if !isdir(p)
        mkpath(p)
    end
    p
end

function get_samples_local_path()
    p = joinpath(homedir(), ".banyan", "samples")
    if !isdir(p)
        mkpath(p)
    end
    p
end

function get_location_source(lp::LocationPath)::Tuple{Location,String,String}
    global s3

    # This checks local cache and S3 cache for sample and metadata files.
    # It then returns a Location object (with a null sample) and the local file names
    # to read/write the metadata and sample from/to.

    # Load in metadata
    metadata_path = get_metadata_path(lp)
    metadata_local_path = joinpath(get_metadata_local_path(), metadata_path)
    metadata_s3_path = "/$(banyan_metadata_bucket_name())/$metadata_path"
    src_params_not_stored_locally = false
    src_params::Dict{String, String} = if isfile(metadata_local_path)
        lm = Dates.unix2datetime(mtime(metadata_local_path))
        if_modified_since_string =
            "$(dayabbr(lm)), $(twodigit(day(lm))) $(monthabbr(lm)) $(year(lm)) $(twodigit(hour(lm))):$(twodigit(minute(lm))):$(twodigit(second(lm))) GMT"
        try
            d = get_src_params_dict_from_arrow(seekstart(s3("GET", metadata_s3_path, Dict("headers" => Dict("If-Modified-Since" => if_modified_since_string))).io))
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
            d = get_src_params_dict_from_arrow(seekstart(s3("GET", metadata_s3_path).io))
            src_params_not_stored_locally = true
            d
        catch e
            if is_debug_on()
                show(e)
            end
            if !AWSExceptionInfo(e).not_found
                @warn "Assuming metadata isn't copied in the cloud because of following error in attempted access"
                show(e)
            end
            Dict{String, String}()
        end
    end
    # Store metadata locally
    if src_params_not_stored_locally && !isempty(src_params)
        Arrow.write(metadata_local_path, Arrow.Table(); metadata=src_params)
    end

    # Load in sample

    sc = get_sampling_config(lp)
    force_new_sample_rate = sc.force_new_sample_rate
    desired_sample_rate = sc.rate
    sample_path_prefix = get_sample_path_prefix(lp)

    # Find local samples
    found_local_samples = Tuple{String,Int64}[]
    found_local_sample_rate_diffs = Int64[]
    sample_local_dir = joinpath(get_samples_local_path(), sample_path_prefix)
    mkpath(sample_local_dir)
    local_sample_paths = isdir(sample_local_dir) ? readdir(sample_local_dir) : String[]
    for local_sample_path_suffix in local_sample_paths
        local_sample_path = joinpath(sample_local_dir, local_sample_path_suffix)
        local_sample_rate = parse(Int64, local_sample_path_suffix)
        diff_sample_rate = abs(local_sample_rate - desired_sample_rate)
        if !force_new_sample_rate || diff_sample_rate == 0
            push!(found_local_samples, (local_sample_path, local_sample_rate))
            push!(found_local_sample_rate_diffs, diff_sample_rate)
        end
    end

    # Sort in descending suitability (the most suitable sample is the one with sample
    # rate closest to the desired sample rate)
    found_local_samples = found_local_samples[sortperm(found_local_sample_rate_diffs)]

    # Find a local sample that is up-to-date. NOTE: The data itself might have
    # changed in which case the cached samples are out-of-date and we don't
    # currently capture that. This doesn't even check if there is a more recent
    # sample of a different sample rate (although that is kind of a bug/limitation
    # that could be resolved though the best way to resolve it would be by
    # comparing to the last modified date for the data itself). It just checks that the remote sample
    # hasn't been manually invalidated by the user or a Banyan writing function
    # and that there isn't a newer sample for this specific sample rate.
    final_local_sample_path = ""
    final_sample_rate = -1
    for (sample_local_path, sample_rate) in found_local_samples
        lm = Dates.unix2datetime(mtime(sample_local_path))
        if_modified_since_string =
            "$(dayabbr(lm)), $(twodigit(day(lm))) $(monthabbr(lm)) $(year(lm)) $(twodigit(hour(lm))):$(twodigit(minute(lm))):$(twodigit(second(lm))) GMT"
        sample_s3_path = "/$(banyan_samples_bucket_name())/$sample_path_prefix/$sample_rate"
        try
            blob = s3("GET", sample_s3_path, Dict("headers" => Dict("If-Modified-Since" => if_modified_since_string)))
            write(sample_local_path, seekstart(blob.io))  # This overwrites the existing file
            final_local_sample_path = sample_local_path
            final_sample_rate = sample_rate
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
                final_sample_rate = sample_rate
                break
            else
                @warn "Assumming locally stored metadata is invalid because of following error in accessing the metadata copy in the cloud"
                show(e)
            end
        end
    end

    # If no such sample is found, search the S3 bucket
    banyan_samples_bucket = S3Path("s3://$(banyan_samples_bucket_name())")
    banyan_samples_object_dir = joinpath(banyan_samples_bucket, sample_path_prefix)
    if isempty(final_local_sample_path)
        final_sample_rate = -1
        for object_key in readdir_no_error(banyan_samples_object_dir)
            object_sample_rate = parse(Int64, object_key)
            object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
            curr_sample_rate_diff = abs(final_sample_rate - desired_sample_rate)
            if force_new_sample_rate ? (object_sample_rate_diff == 0) : (final_sample_rate == -1 || object_sample_rate_diff < curr_sample_rate_diff)
                final_sample_rate = object_sample_rate
                final_local_sample_path = joinpath(sample_local_dir, object_key)
            end
        end
        if final_sample_rate != -1
            cp(
                joinpath(
                    banyan_samples_bucket,
                    sample_path_prefix,
                    string(final_sample_rate)
                ),
                Path(final_local_sample_path)
            )
        end
    end
    
    # Construct and return LocationSource
    res_location = LocationSource(
        get(src_params, "name", "Remote"),
        src_params,
        parse(Int64, get(src_params, "sample_memory_usage", "0")),
        deepcopy(NOTHING_SAMPLE)
    )
    res_location.metadata_invalid = isempty(src_params)
    res_location.sample_invalid = isempty(final_local_sample_path)
    final_sample_rate = isempty(final_local_sample_path) ? desired_sample_rate : final_sample_rate
    (
        res_location,
        metadata_local_path,
        joinpath(sample_local_dir, string(final_sample_rate))
    )
end