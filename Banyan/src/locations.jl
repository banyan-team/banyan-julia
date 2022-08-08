#################
# Location type #
#################

const NOTHING_LOCATION = Location("None", "None", LocationParameters(), LocationParameters(), Int64(-1), NOTHING_SAMPLE, false, false)

const INVALID_LOCATION = Location("None", "None", LocationParameters(), LocationParameters(), Int64(-1), NOTHING_SAMPLE, true, true)

Location(name::String, parameters::LocationParameters, total_memory_usage::Int64 = -1, sample::Sample = Sample())::Location =
    Location(name, name, parameters, parameters, total_memory_usage, sample, false, false)

Base.isnothing(l::Location) = isnothing(l.sample)

LocationSource(name::String, parameters::LocationParameters, total_memory_usage::Int64 = -1, sample::Sample = Sample())::Location =
    Location(name, "None", parameters, LocationParameters(), total_memory_usage, sample, false, false)

LocationDestination(
    name::String,
    parameters::LocationParameters
)::Location = Location("None", name, LocationParameters(), parameters, -1, Sample(), false, false)

function to_jl(lt::Location)
    return Dict{String,Any}(
        "src_name" => lt.src_name,
        "dst_name" => lt.dst_name,
        "src_parameters" => lt.src_parameters,
        "dst_parameters" => lt.dst_parameters,
        # NOTE: sample.properties[:rate] is always set in the Sample
        # constructor to the configured sample rate (default 1/nworkers) for
        # this session
        # TODO: Instead of computing the total memory usage here, compute it
        # at the end of each `@partitioned`. That way we will count twice for
        # mutation
        "total_memory_usage" => lt.total_memory_usage == -1 ? nothing : lt.total_memory_usage,
    )
end



################################
# Methods for setting location #
################################

sourced(fut::AbstractFuture, loc::Location) = sourced(convert(Future, fut)::Future, loc)
function sourced(fut::Future, loc::Location)
    if isnothing(loc.src_name)
        error("Location cannot be used as a source")
    end

    fut_location::Location = get_location(fut)
    # Every future must have a location unless this is the future constructor
    # that's calling this and setting the source location without the
    # desination being set yet.
    if isnothing(fut_location)
        located(
            fut,
            Location(
                loc.src_name,
                "None",
                loc.src_parameters,
                Dict{String,Any}(),
                loc.total_memory_usage,
                if !isnothing(loc.sample.value)
                    # If this location is like some remote location, then we need
                    # a sample from it.
                    loc.sample
                else
                    # Otherwise just make a fresh new sample.
                    Sample()
                end,
                loc.metadata_invalid,
                loc.sample_invalid
            ),
        )
    else
        fut_location::Location
        located(
            fut,
            Location(
                loc.src_name,
                fut_location.dst_name,
                loc.src_parameters,
                fut_location.dst_parameters,
                loc.total_memory_usage,
                if !isnothing(loc.sample.value)
                    # If this location is like some remote location, then we need
                    # a sample from it.
                    loc.sample
                else
                    # Maybe we are just declaring that this future is sourced from
                    # disk on the cluster. In that case, just use the existing
                    # location if there is one.
                    fut_location.sample
                end,
                loc.metadata_invalid,
                loc.sample_invalid
            ),
        )
    end
end

destined(fut::AbstractFuture, loc::Location) = destined(convert(Future, fut)::Future, loc)
function destined(fut::Future, loc::Location)
    if isnothing(loc.dst_name)
        error("Location cannot be used as a destination")
    end

    fut_location::Location = get_location(fut)
    if isnothing(fut_location)
        located(
            fut,
            Location(
                "None",
                loc.dst_name,
                EMPTY_DICT,
                loc.dst_parameters,
                fut_location.total_memory_usage,
                Sample(),
                loc.metadata_invalid,
                loc.sample_invalid
            ),
        )
    else
        fut_location::Location
        located(
            fut,
            Location(
                fut_location.src_name,
                loc.dst_name,
                fut_location.src_parameters,
                loc.dst_parameters,
                fut_location.total_memory_usage,
                fut_location.sample,
                fut_location.metadata_invalid,
                fut_location.sample_invalid
            ),
        )
    end
end

# The purspose of making the source and destination assignment lazy is because
# location constructors perform sample collection and collecting samples is
# expensive. So after we write to a location and invalidate the cached sample,
# we only want to compute the new location source if the value is really used
# later on.

global source_location_funcs = Dict{ValueId,Function}()
global destination_location_funcs = Dict{ValueId,Function}()

function sourced(fut::Future, @nospecialize(location_func::Function))
    global source_location_funcs
    source_location_funcs[fut.value_id] = location_func
end

function destined(fut::Future, @nospecialize(location_func::Function))
    global destination_location_funcs
    destination_location_funcs[fut.value_id] = location_func
end

function apply_sourced_or_destined_funcs(fut::Future)
    global source_location_funcs
    global destination_location_funcs
    source_location_funcs::Dict{ValueId,Function}
    destination_location_funcs::Dict{ValueId,Function}
    if haskey(source_location_funcs, fut.value_id)
        src_func::Function = source_location_funcs[fut.value_id]
        new_source_location::Location = src_func(fut)
        sourced(fut, new_source_location)
        pop!(source_location_funcs, fut.value_id)
    end
    if haskey(destination_location_funcs, fut.value_id)
        dst_func::Function = destination_location_funcs[fut.value_id]
        new_destination_location::Location = dst_func(fut)
        destined(fut, new_destination_location)
        pop!(destination_location_funcs, fut.value_id)
    end
end

function located(fut::Future, location::Location)
    session = get_session()
    value_id = fut.value_id

    # Store future's datatype in the parameters so that it could be used for
    # dispatching various PFs (partitioning functions).
    location.src_parameters["datatype"] = fut.datatype
    location.dst_parameters["datatype"] = fut.datatype

    if location.src_name == "Client" || location.dst_name == "Client"
        session.futures_on_client[value_id] = fut
    else
        # TODO: Set loc of all Futures with Client loc to None at end of
        # evaluate and ensure that this is proper way to handle Client
        delete!(session.futures_on_client, value_id)
    end

    session.locations[value_id] = location
    record_request(RecordLocationRequest(value_id, location))
end

################################
# Methods for getting location #
################################

get_src_name(fut)::String = get_location(fut).src_name
get_dst_name(fut)::String = get_location(fut).dst_name
get_src_parameters(fut)::LocationParameters = get_location(fut).src_parameters
get_dst_parameters(fut)::LocationParameters = get_location(fut).dst_parameters

####################
# Simple locations #
####################

function Value(val::T)::Location where {T}
    LocationSource("Value", Dict{String,Any}("value" => to_jl_value(val)), total_memory_usage(val), ExactSample(val))
end

# TODO: Implement Size
Size(val)::Location = LocationSource(
    "Value",
    Dict{String,Any}("value" => to_jl_value(val)),
    0,
    Sample(indexapply(getsamplenrows, val, 1), 1),
)

function Client(val::T)::Location where {T}
    LocationSource("Client", Dict{String,Any}(), total_memory_usage(val), ExactSample(val))
end
const CLIENT = Location("None", "Client", LocationParameters(), LocationParameters(), Int64(0), Sample(nothing, Int64(0), Int64(1)), false, false)
Client()::Location = deepcopy(CLIENT)
# TODO: Un-comment only if Size is needed
# Size(size) = Value(size)
const NONE_LOCATION = Location("None", "None", LocationParameters(), LocationParameters(), Int64(0), Sample(nothing, Int64(0), Int64(1)), false, false)
None()::Location = deepcopy(NONE_LOCATION)
# The scheduler intelligently determines when to split from and merge to disk even when no location is specified
const DISK = NONE_LOCATION
Disk()::Location = deepcopy(DISK)
# Values assigned "None" location as well as other locations may reassigned
# "Memory" or "Disk" locations by the scheduler depending on where the relevant
# data is.

# NOTE: Currently, we only support s3:// or http(s):// and only either a
# single file or a directory containing files that comprise the dataset.
# What we currently support:
# - Single HDF5 files (with .h5 or .hdf5 extension) with group at end of name
# - Single CSV/Parquet/Arrow files (with appropraite extensions)
# - Directories containing CSV/Parquet/Arrow files
# - s3:// or http(s):// (but directories and writing are not supported over the
# Internet)

# TODO: Add support for Client

# TODO: Implement Client, Remote for HDF5, Parquet, Arrow, and CSV so that they
# compute nrows ()

####################
# Remote locations #
####################

# NOTE: Sampling may be the source of weird and annoying bugs for users.
# Different values tracked by Banyan might have different sampling rates
# where one is the session's set sampling rate and the other has a sampling rate
# of 1. If it is expected for both the values to have the same size or be
# equivalent in some way, this won't be the case. The samples will have
# differerent size.
# 
# Another edge case is when you have two dataframes each stored in S3 and they
# have the same number of rows and the order matters in a way that each row
# corresponds to the row at the same index in the other dataframe. We can get
# around this by using the same seed for every value we read in.
# 
# Aside from these edge cases, we should be mostly okay though. We simply hold
# on to the first 1024 data points. And then swap stuff out randomly. We
# ensure that the resulting sample size is deterministaclly produced from the
# overall data size. This way, two arrays that have the same actual size will
# be guaranteed to have the same sample size.

# Things to think about when choosing max sample length
# - You might have massive 2D (or even higher dimensional) arrays
# - You might have lots of huge images
# - You might have lots of workers so your sample rate is really large

MAX_EXACT_SAMPLE_LENGTH = parse(Int64, get(ENV, "BANYAN_MAX_EXACT_SAMPLE_LENGTH", "1024")::String)
get_max_exact_sample_length()::Int64 = MAX_EXACT_SAMPLE_LENGTH
function set_max_exact_sample_length(val)
    global MAX_EXACT_SAMPLE_LENGTH
    MAX_EXACT_SAMPLE_LENGTH = val
end

getsamplenrows(totalnrows::Int64)::Int64 =
    if totalnrows <= get_max_exact_sample_length()
        # NOTE: This includes the case where the dataset is empty
        # (totalnrows == 0)
        totalnrows
    else
        # Must have at least 1 row
        cld(totalnrows, get_sample_rate())
    end

# We maintain a cache of locations and a cache of samples. Locations contain
# information about what files are in the dataset and how many rows they have
# while samples contain an actual sample from that dataset

# The invalidate_* and invalidate_all_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.

_invalidate_all_locations() = begin
    for dir_name in ["banyan_locations", "banyan_meta"]
        rm("s3/$(get_cluster_s3_bucket_name())/$dir_name/", force=true, recursive=true)
    end
end
_invalidate_metadata(remotepath) =
    let p = get_location_path(remotepath)
        if isfile(p)
            loc = deserialize_retry(p)
            loc.metadata_invalid = true
            serialize(p, loc)
        end
    end
_invalidate_sample(remotepath) =
    let p = get_location_path(remotepath)
        if isfile(p)
            loc = deserialize_retry(p)
            loc.sample_invalid = true
            serialize(p, loc)
        end
    end
invalidate_all_locations() = offloaded(_invalidate_all_locations)
invalidate_metadata(p) = offloaded(_invalidate_metadata, p)
invalidate_sample(p) = offloaded(_invalidate_sample, p)

@specialize

# Helper functions for location constructors; these should only be called from the main worker

# TODO: Hash in a more general way so equivalent paths hash to same value
# This hashes such that an extra slash at the end won't make a difference``
get_remotepath_id(remotepath::String) =
    (get_julia_version(), (remotepath |> splitpath |> joinpath)) |> hash
get_remotepath_id(remotepath) = (get_julia_version(), remotepath) |> hash
function get_location_path(remotepath, remotepath_id)
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if !isdir("s3/$session_s3_bucket_name/banyan_locations/")
        mkdir("s3/$session_s3_bucket_name/banyan_locations/")
    end
    "s3/$session_s3_bucket_name/banyan_locations/$(remotepath_id)"
end
function get_meta_path(remotepath, remotepath_id)
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if !isdir("s3/$session_s3_bucket_name/banyan_meta/")
        mkdir("s3/$session_s3_bucket_name/banyan_meta/")
    end
    "s3/$session_s3_bucket_name/banyan_meta/$remotepath_id"
end
get_location_path(remotepath) =
    get_location_path(remotepath, get_remotepath_id(remotepath))
get_meta_path(remotepath) =
    get_meta_path(remotepath, get_remotepath_id(remotepath))

function get_cached_location(remotepath, remotepath_id, metadata_invalid, sample_invalid)
    Random.seed!(hash((get_session_id(), remotepath_id)))
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    location_path = "s3/$session_s3_bucket_name/banyan_locations/$remotepath_id"

    curr_location::Location = try
        deserialize_retry(location_path)
    catch
        INVALID_LOCATION
    end
    curr_location.sample_invalid = curr_location.sample_invalid || sample_invalid
    curr_location.metadata_invalid = curr_location.metadata_invalid || metadata_invalid
    curr_sample_invalid = curr_location.sample_invalid
    curr_metadata_invalid = curr_location.metadata_invalid
    curr_location, curr_sample_invalid, curr_metadata_invalid
end

get_cached_location(remotepath, metadata_invalid, sample_invalid) =
    get_cached_location(remotepath, get_remotepath_id(remotepath), metadata_invalid, sample_invalid)

function cache_location(remotepath, remotepath_id, location_res::Location, invalidate_sample, invalidate_metadata)
    location_path = get_location_path(remotepath, remotepath_id)
    location_to_write = deepcopy(location_res)
    location_to_write.sample_invalid = location_to_write.sample_invalid || invalidate_sample
    location_to_write.metadata_invalid = location_to_write.metadata_invalid || invalidate_metadata
    serialize(location_path, location_to_write)
end
cache_location(remotepath, location_res::Location, invalidate_sample, invalidate_metadata) =
    cache_location(remotepath, get_remotepath_id(remotepath), location_res, invalidate_sample, invalidate_metadata)

# Functions to be extended for different data formats

function sample_from_range(r, sample_rate)
    # TODO: Maybe return whole range if sample rate is 1
    len = length(r)
    sample_len = ceil(Int64, len / sample_rate)
    rand_indices = randsubseq(1:len, 1/sample_rate)
    if length(rand_indices) > sample_len
        rand_indices = rand_indices[1:sample_len]
    else
        while length(rand_indices) < sample_len
            new_index = rand(1:sample_len)
            if !(new_index in rand_indices)
                push!(rand_indices, new_index)
            end
        end
    end
    rand_indices
end

has_separate_metadata(::Val{:jl}) = false
get_metadata(::Val{:jl}, p) = size(deserialize_retry(p), 1)
get_sample_from_data(data, sample_rate, len::Int64) =
    get_sample_from_data(data, sample_rate, sample_from_range(1:len, sample_rate))
function get_sample_from_data(data, sample_rate, rand_indices::Vector{Int64})
    if sample_rate == 1.0
        return data
    end
    data_ndims = ndims(data)
    data_selector::Base.Vector{Union{Colon,Base.Vector{Int64}}} = Base.fill(Colon(), data_ndims)
    data_selector[1] = rand_indices
    data[data_selector...]
end
function get_sample(::Val{:jl}, p, sample_rate, len)
    data = deserialize_retry(p)
    get_sample_from_data(data, sample_rate, len)
end
function get_sample_and_metadata(::Val{:jl}, p, sample_rate)
    data = deserialize_retry(p)
    get_sample_from_data(data, sample_rate, size(data, 1)), size(data, 1)
end

function RemoteSource(
    lp::LocationPath,
    _remote_source::Function,
    load_sample::Function,
    load_sample_after_offloaded::Function,
    write_sample::Function,
    args...
)::Location
    # _remote_table_source(lp::LocationPath, loc::Location, sample_rate::Int64)::Location
    # load_sample accepts a file path
    # load_sample_after_offloaded accepts the sampled value returned by the offloaded function
    # (for BDF.jl, this is an Arrow blob of bytes that needs to be converted into an actual
    # dataframe once sent to the client side)
    
    # Look at local and S3 caches of metadata and samples to attempt to
    # construct a Location.
    loc, local_metadata_path, local_sample_path = get_location_source(lp)
    sc = get_sampling_config(lp)
    sc.rate = parse_sample_rate(local_sample_path)

    if !loc.metadata_invalid && !loc.sample_invalid
        # Case where both sample and parameters are valid
        loc.sample.value = load_sample(local_sample_path)
        loc
    elseif loc.metadata_invalid && !loc.sample_invalid
        # Case where parameters are invalid
        new_loc = offloaded(_remote_source, lp, loc, sc, args...; distributed=true)
        Arrow.write(local_metadata_path, Arrow.Table(); metadata=new_loc.src_parameters)
        new_loc.sample.value = load_sample(local_sample_path)
        new_loc
    else
        # Case where sample is invalid

        # Get the Location with up-to-date metadata (source parameters) and sample
        new_loc = offloaded(_remote_source, lp, loc, sc, args...; distributed=true)

        if !loc.metadata_invalid
            # Store the metadata locally. The local copy just has the source
            # parameters but PFs can still access the S3 copy which will have the
            # table of file names and #s of rows.
            Arrow.write(local_metadata_path, Arrow.Table(); metadata=new_loc.src_parameters)
        end

        # Store the Arrow sample locally and update the returned Sample
        write_sample(local_sample_path, new_loc.sample.value)
        new_loc.sample.value = load_sample_after_offloaded(new_loc.sample.value)
        
        new_loc
    end
end