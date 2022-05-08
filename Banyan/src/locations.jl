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

# function Base.getproperty(loc::Location, name::Symbol)
#     if hasfield(Location, name)
#         return getfield(loc, name)
#     end

#     n = string(name)
#     # `n` might exist in both the source parameters _AND_ the destination
#     # parameters but we prioritize the source.
#     if haskey(loc.src_parameters, n)
#         loc.src_parameters[n]
#     elseif haskey(loc.dst_parameters, n)
#         loc.dst_parameters[n]
#     else
#         error("$name not found among location source $(loc.src_name) with parameters $(loc.src_parameters) and destination $(loc.dst_name) with parameters $(loc.dst_parameters)")
#     end
# end

# function Base.hasproperty(loc::Location, name::Symbol)
#     n = string(name)
#     hasfield(Location, name) || haskey(loc.src_parameters, n) || haskey(loc.dst_parameters, n)
# end

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
                loc.parameters_invalid,
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
                loc.parameters_invalid,
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
                Dict{String,Any},
                loc.dst_parameters,
                fut_location.total_memory_usage,
                Sample(),
                loc.parameters_invalid,
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
                fut_location.parameters_invalid,
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
    # @debug size(location.sample.value)
end

# function located(futs...)
#     futs = futs .|> obj -> convert(Future, obj)
#     maxindfuts = argmax([get_location(f).total_memory_usage for f in futs])
#     for fut in futs
#         located(fut, get_location(futs[maxindfuts]))
#     end
# end

# NOTE: The below operations (mem and val) should rarely be used if every. We
# should probably even remove them at some point. Memory usage of each sample
# is automatically detected and stored. If you want to make a future have Value
# location type, simply use the `Future` constructor and pass your value in.

# NOTE: The below mem and val are not used anywhere.

# function mem(fut, estimated_total_memory_usage::Int64)
#     fut = convert(Future, fut)
#     location = get_location(fut)
#     location.total_memory_usage = estimated_total_memory_usage
#     record_request(RecordLocationRequest(fut.value_id, location))
# end

# mem(fut, n::Int64, ty::DataType) = mem(fut, n * sizeof(ty))
# mem(fut) = mem(fut, sizeof(convert(Future, fut).value))

# function mem(futs...)
#     for fut in futs
#         mem(fut, maximum([
#             begin
#                 get_location(f).total_memory_usage
#             end for f in futs
#         ]))
#     end
# end

# val(fut) = located(fut, Value(convert(Future, fut).value))

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
    Sample(indexapply(getsamplenrows, val, 1)),
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

MAX_EXACT_SAMPLE_LENGTH = parse(Int64, get(ENV, "BANYAN_MAX_EXACT_SAMPLE_LENGTH", "2048")::String)
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
        cld(totalnrows, get_session().sample_rate)
    end

# We maintain a cache of locations and a cache of samples. Locations contain
# information about what files are in the dataset and how many rows they have
# while samples contain an actual sample from that dataset

# The invalidate_* and invalidate_all_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.

_invalidate_all_locations() = rm("s3/$(get_cluster_s3_bucket_name())/banyan_locations/", force=true, recursive=true)
_invalidate_metadata(remotepath) =
    let p = get_location_path(remotepath)
        if isfile(p)
            loc = deserialize(p)
            loc.parameters_invalid = true
        end
    end
_invalidate_sample(remotepath) =
    let p = get_location_path(remotepath)
        if isfile(p)
            loc = deserialize(p)
            loc.sample_invalid = true
        end
    end
invalidate_all_locations() = offloaded(_invalidate_all_locations)
invalidate_metadata(p) = offloaded(_invalidate_metadata, p)
invalidate_sample(p) = offloaded(_invalidate_sample, p)

# Getting remote location info and samples is not easy. So we cache it and
# allow a function to be passed in to actually process the given path and get
# the location if needed.

# It's not just simple caching. The function for actually getting the
# location/sample may only need to get the sample or only the location
# info and if just the location info or just the sample is cached

@nospecialize

# function RemoteSource(
#     @nospecialize(get_remote_source::Function),
#     p,
#     shuffled::Bool,
#     metadata_invalid::Bool,
#     sample_invalid::Bool,
#     invalidate_metadata::Bool,
#     invalidate_sample::Bool
# )::Location
#     # In the context of remote locations, the location refers to just the non-sample part of it; i.e., the
#     # info about what files are in the dataset and how many rows they have.

#     # Set shuffled to true if the data is fully shuffled and so we can just take
#     # the first rows of the data to get our sample.
#     # Set similar_files to true if the files are simialr enough (in their distribution
#     # of data) that we can just randomly select files and only sample from those.
#     # Invalidate the location only when the number of files or their names or the
#     # number of rows they have changes. If you just update a few rows, there's no
#     # need to invalidate the location.
#     # Invalidate the sample only when the data has drifted enough. So if you update
#     # a row to have an outlier or if you add a whole bunch of new rows, make sure
#     # to udpate the sample.

#     # TODO: Store the time it took to collect a sample so that the user is
#     # warned when invalidating a sample that took a long time to collect

#     # TODO: Document the caching behavior better
#     # Read location from cache. The location will include metadata like the
#     # number of rows in each file as well as a sample that can be used on the
#     # client side for estimating memory usage and data skew among other things.
#     # Get paths with cached locations and samples
#     locationspath = joinpath(homedir(), ".banyan", "sources")
#     samplespath = joinpath(homedir(), ".banyan", "samples")
#     p_hash_string::String = string(hash(p))
#     locationpath = joinpath(locationspath, p_hash_string)
#     samplepath = joinpath(samplespath, p_hash_string)

#     # Get cached sample if it exists
#     remote_sample::Sample = if isfile(samplepath) && !sample_invalid
#         deserialize(samplepath)
#     else
#         NOTHING_SAMPLE
#     end

#     # Get cached location if it exists
#     remote_source::Location = if isfile(locationpath) && !metadata_invalid
#         deserialize(locationpath)
#     else
#         NOTHING_LOCATION
#     end
#     remote_source = get_remote_source_cached(get_remote_source, p, remote_source, remote_sample, shuffled)
#     remote_sample = remote_source.sample

#     if Banyan.INVESTIGATING_CACHING_LOCATION_INFO || Banyan.INVESTIGATING_CACHING_SAMPLES
#         println("In RemoteSource")
#         @show locationpath samplepath isfile(samplepath) sample_invalid isfile(locationpath) metadata_invalid
#     end

#     # Store location in cache. The same logic below applies to having a
#     # `&& isempty(remote_source.files)` which effectively allows us
#     # to reuse the location (computed files and row lengths) but only if the
#     # location was actually already written to. If this is the first time we
#     # are calling `write_parquet` with `invalidate_metadata=false`, then
#     # the location will not be saved. But on future writes, the first write's
#     # location will be used.
#     remote_source_src_name::String = remote_source.src_name
#     remote_source_nbytes::Int64 = remote_source.src_parameters["nbytes"]
#     if !invalidate_metadata && remote_source_src_name == "Remote" && remote_source_nbytes > 0
#         mkpath(locationspath)
#         serialize(locationpath, remote_source)
#     else
#         rm(locationpath, force=true, recursive=true)
#     end

#     # Store sample in cache. We don't store null samples because they are
#     # either samples for locations that don't exist yet (write-only) or are
#     # really cheap to collect the sample. Yes, it is true that generally we
#     # will invalidate the sample on reads but for performance reasons someone
#     # might not. But if they don't invalidate the sample, we only want to reuse
#     # the sample if it was for a location that was actually written to.
#     if !invalidate_sample && !isnothing(remote_sample)
#         mkpath(samplespath)
#         serialize(samplepath, remote_sample)
#     else
#         rm(samplepath, force=true, recursive=true)
#     end

#     remote_source
# end

# function RemoteDestination(
#     @nospecialize(get_remote_destination::Function),
#     p::String;
#     invalidate_metadata::Bool = true,
#     invalidate_sample::Bool = true
# )::Location
#     p_hash_string = string(hash(p))
#     if invalidate_metadata
#         rm(joinpath(homedir(), ".banyan", "sources", p_hash_string), force=true, recursive=true)
#     end
#     if invalidate_sample
#         rm(joinpath(homedir(), ".banyan", "samples", p_hash_string), force=true, recursive=true)
#     end

#     get_remote_destination(p)::Location
# end

# function get_remote_source_cached(get_remote_source, remotepath, remote_source, remote_sample, shuffled)::Location
#     # If both the location and sample are already cached, just return them
#     if !isnothing(remote_source) && !isnothing(remote_sample)
#         remote_source.sample = remote_sample
#         return remote_source
#     end

#     # This is so that we can make sure that any random selection fo rows is
#     # deterministic. Might not be needed...
#     Random.seed!(hash(get_session_id()))

#     get_remote_source(
#         remotepath,
#         Base.inferencebarrier(remote_source),
#         Base.inferencebarrier(remote_sample),
#         Base.inferencebarrier(shuffled)
#     )
# end

# function get_remote_destination(remotepath)::Location
#     # Handle HDF5 paths (which include the dataset in the path)
#     remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)

#     # Return either an HDF5 location or a table location
#     if isa_hdf5
#         get_remote_hdf5_destination(remotepath, datasetpath)
#     else
#         get_remote_table_destination(remotepath)
#     end
# end

# function convert_to_unpooled(A::T) where {T}
#     type_name = typeof(A).name.name
#     if type_name == :PooledArray
#         Base.collect(A)
#     elseif type_name == :CategoricalArray
#         unwrap.(A)
#     else
#         # For handling SentinelArrays.MissingVector
#         Base.convert(Array, A)
#     end
# end

@specialize

# Helper functions for location constructors; these should only be called from the main worker

# TODO: Hash in a more general way so equivalent paths hash to same value
# This hashes such that an extra slash at the end won't make a difference``
get_remotepath_id(remotepath::String) =
    remotepath |> splitpath |> joinpath |> hash
get_remotepath_id(remotepath) = remotepath |> hash
function get_location_path(remotepath)
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if !isdir("s3/$session_s3_bucket_name/banyan_locations/")
        mkdir("s3/$session_s3_bucket_name/banyan_locations/")
    end
    "s3/$session_s3_bucket_name/banyan_locations/$(get_remotepath_id(remotepath))"
end
function get_meta_path(remotepath)
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if !isdir("s3/$session_s3_bucket_name/banyan_meta/")
        mkdir("s3/$session_s3_bucket_name/banyan_meta/")
    end
    "s3/$session_s3_bucket_name/banyan_meta/$(get_remotepath_id(remotepath))"
end

function get_cached_location(remotepath, metadata_invalid, sample_invalid)
    remotepath_id = get_remotepath_id(remotepath)
    Random.seed!(hash(get_session_id(), remotepath_id))
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    location_path = "s3/$session_s3_bucket_name/banyan_locations/$remotepath_id"
    curr_location::Location = isfile(location_path) ? deserialize(location_path) : INVALID_LOCATION
    curr_location.sample_invalid = curr_location.sample_invalid || sample_invalid
    curr_location.parameters_invalid = curr_location.parameters_invalid || metadata_invalid
    curr_sample_invalid = curr_location.sample_invalid
    curr_parameters_invalid = curr_location.parameters_invalid
    curr_location, curr_sample_invalid, curr_parameters_invalid
end

function cache_location(remotepath, location_res::Location, invalidate_sample, invalidate_metadata)
    location_path = get_location_path(remotepath)
    location_to_write = deepcopy(location_res)
    location_to_write.sample_invalid = location_to_write.sample_invalid || invalidate_sample
    location_to_write.parameters_invalid = location_to_write.parameters_invalid || invalidate_metadata
    serialize(location_path, location_to_write)
end

# Functions to be extended for different data formats

function sample_from_range(r, sample_rate)
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
get_metadata(::Val{:jl}, p) = size(deserialize(p), 1)
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
    data = deserialize(p)
    get_sample_from_data(data, sample_rate, len)
end
function get_sample_and_metadata(::Val{:jl}, p, sample_rate)
    data = deserialize(p)
    get_sample_from_data(data, sample_rate, size(data, 1)), size(data, 1)
end