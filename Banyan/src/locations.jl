#################
# Location type #
#################

const LocationParameters = Dict{String,Any}

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::String
    dst_name::String
    src_parameters::LocationParameters
    dst_parameters::LocationParameters
    total_memory_usage::Union{Integer,Nothing}
    sample::Sample

    function Location(
        src_name::String,
        dst_name::String,
        src_parameters::Dict{String,<:Any},
        dst_parameters::Dict{String,<:Any},
        total_memory_usage::Union{Integer,Nothing} = nothing,
        sample::Sample = Sample(),
    )
        # NOTE: A file might be None and None if it is simply to be cached on
        # disk and then read from
        # if src_name == "None" && dst_name == "None"
        #     error(
        #         "Location must either be usable as a source or as a destination for data",
        #     )
        # end

        new(
            src_name,
            dst_name,
            src_parameters,
            dst_parameters,
            total_memory_usage,
            sample
        )
    end
end

Location(name::String, parameters::Dict{String,<:Any}, total_memory_usage::Union{Integer,Nothing} = nothing, sample::Sample = Sample()) =
    Location(name, name, parameters, parameters, total_memory_usage, sample)

LocationSource(name::String, parameters::Dict{String,<:Any}, total_memory_usage::Union{Integer,Nothing} = nothing, sample::Sample = Sample()) =
    Location(name, "None", parameters, LocationParameters(), total_memory_usage, sample)

LocationDestination(
    name::String,
    parameters::Dict{String,<:Any}
) = Location("None", name, LocationParameters(), parameters, nothing)

function Base.getproperty(loc::Location, name::Symbol)
    if hasfield(Location, name)
        return getfield(loc, name)
    end

    n = string(name)
    # `n` might exist in both the source parameters _AND_ the destination
    # parameters but we prioritize the source.
    if haskey(loc.src_parameters, n)
        loc.src_parameters[n]
    elseif haskey(loc.dst_parameters, n)
        loc.dst_parameters[n]
    else
        error("$name not found among location source $(loc.src_name) with parameters $(loc.src_parameters) and destination $(loc.dst_name) with parameters $(loc.dst_parameters)")
    end
end

function Base.hasproperty(loc::Location, name::Symbol)
    n = string(name)
    hasfield(Location, name) || haskey(loc.src_parameters, n) || haskey(loc.dst_parameters, n)
end

# Accessing the sample of a location
sample(loc::Location) = sample(loc.sample)

function to_jl(lt::Location)
    return Dict(
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
        "total_memory_usage" => lt.total_memory_usage,
    )
end



################################
# Methods for setting location #
################################

function sourced(fut, loc::Location)
    if isnothing(loc.src_name)
        error("Location cannot be used as a source")
    end

    fut::Future = convert(Future, fut)
    fut_location = get_location(fut)
    # Every future must have a location unless this is the future constructor
    # that's calling this and setting the source location without the
    # desination being set yet.
    located(
        fut,
        Location(
            loc.src_name,
            isnothing(fut_location) ? "None" : fut_location.dst_name,
            loc.src_parameters,
            isnothing(fut_location) ? Dict{String,Any}() : fut_location.dst_parameters,
            loc.total_memory_usage,
            if !isnothing(loc.sample.value)
                # If this location is like some remote location, then we need
                # a sample from it.
                loc.sample
            elseif !isnothing(fut_location)
                # Maybe we are just declaring that this future is sourced from
                # disk on the cluster. In that case, just use the existing
                # location if there is one.
                fut_location.sample
            else
                # Otherwise just make a fresh new sample.
                Sample()
            end,
        ),
    )
end

function destined(fut, loc::Location)
    if isnothing(loc.dst_name)
        error("Location cannot be used as a destination")
    end

    fut::Future = convert(Future, fut)
    fut_location = get_location(fut.value_id)
    located(
        fut,
        Location(
            isnothing(fut_location) ? "None" : fut_location.src_name,
            loc.dst_name,
            isnothing(fut_location) ? Dict{String,Any}() : fut_location.src_parameters,
            loc.dst_parameters,
            fut_location.total_memory_usage,
            isnothing(fut_location) ? Sample() : fut_location.sample,
        ),
    )
end

# The purspose of making the source and destination assignment lazy is because
# location constructors perform sample collection and collecting samples is
# expensive. So after we write to a location and invalidate the cached sample,
# we only want to compute the new location source if the value is really used
# later on.

global source_location_funcs = Dict()
global destination_location_funcs = Dict()

function sourced(fut, location_func::Function)
    global source_location_funcs
    source_location_funcs[fut.value_id] = location_func
end

function destined(fut, location_func::Function)
    global destination_location_funcs
    destination_location_funcs[fut.value_id] = location_func
end

function apply_sourced_or_destined_funcs(fut)
    global source_location_funcs
    global destination_location_funcs
    if haskey(source_location_funcs, fut.value_id)
        sourced(fut, source_location_funcs[fut.value_id](fut))
        pop!(source_location_funcs, fut.value_id)
    end
    if haskey(destination_location_funcs, fut.value_id)
        destined(fut, source_location_funcs[fut.value_id](fut))
        pop!(destination_location_funcs, fut.value_id)
    end
end

function located(fut, location::Location)
    session = get_session()
    fut = convert(Future, fut)
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

function located(futs...)
    futs = futs .|> obj -> convert(Future, obj)
    maxindfuts = argmax([get_location(f).total_memory_usage for f in futs])
    for fut in futs
        located(fut, get_location(futs[maxindfuts]))
    end
end

# NOTE: The below operations (mem and val) should rarely be used if every. We
# should probably even remove them at some point. Memory usage of each sample
# is automatically detected and stored. If you want to make a future have Value
# location type, simply use the `Future` constructor and pass your value in.

# NOTE: The below mem and val are not used anywhere.

function mem(fut, estimated_total_memory_usage::Integer)
    fut = convert(Future, fut)
    location = get_location(fut)
    location.total_memory_usage = estimated_total_memory_usage
    record_request(RecordLocationRequest(fut.value_id, location))
end

mem(fut, n::Integer, ty::DataType) = mem(fut, n * sizeof(ty))
mem(fut) = mem(fut, sizeof(convert(Future, fut).value))

function mem(futs...)
    for fut in futs
        mem(fut, maximum([
            begin
                get_location(f).total_memory_usage
            end for f in futs
        ]))
    end
end

val(fut) = located(fut, Value(convert(Future, fut).value))

################################
# Methods for getting location #
################################

get_src_name(fut) = get_location(fut).src_name
get_dst_name(fut) = get_location(fut).dst_name
get_src_parameters(fut) = get_location(fut).src_parameters
get_dst_parameters(fut) = get_location(fut).dst_parameters

####################
# Simple locations #
####################

Value(val) = LocationSource("Value", Dict("value" => to_jl_value(val)), total_memory_usage(val), ExactSample(val))

# TODO: Implement Size
Size(val) = LocationSource(
    "Value",
    Dict("value" => to_jl_value(val)),
    0,
    Sample(indexapply(getsamplenrows, val, index = 1)),
)

Client(val) = LocationSource("Client", Dict{String,Any}(), total_memory_usage(val), ExactSample(val))
Client() = LocationDestination("Client", Dict{String,Any}())
# TODO: Un-comment only if Size is needed
# Size(size) = Value(size)

None() = Location("None", Dict{String,Any}(), 0)
Disk() = Location("None", Dict{String,Any}()) # The scheduler intelligently determines when to split from and merge to disk even when no location is specified
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

get_max_exact_sample_length() = parse(Int, get(ENV, "BANYAN_MAX_EXACT_SAMPLE_LENGTH", "2048"))

getsamplenrows(totalnrows) =
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

# The invalidate_* and clear_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.

clear_sources() = rm(joinpath(homedir(), ".banyan", "sources"), force=true, recursive=true)
clear_samples() = rm(joinpath(homedir(), ".banyan", "samples"), force=true, recursive=true)
invalidate_source(p) = rm(joinpath(homedir(), ".banyan", "sources", p |> hash |> string), force=true, recursive=true)
invalidate_sample(p) = rm(joinpath(homedir(), ".banyan", "samples", p |> hash |> string), force=true, recursive=true)

# Getting remote location info and samples is not easy. So we cache it and
# allow a function to be passed in to actually process the given path and get
# the location if needed.

# It's not just simple caching. The function for actually getting the
# location/sample may only need to get the sample or only the location
# info and if just the location info or just the sample is cached

function RemoteSource(get_remote_source, p; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)
    # In the context of remote locations, the location refers to just the non-sample part of it; i.e., the
    # info about what files are in the dataset and how many rows they have.

    # Set shuffled to true if the data is fully shuffled and so we can just take
    # the first rows of the data to get our sample.
    # Set similar_files to true if the files are simialr enough (in their distribution
    # of data) that we can just randomly select files and only sample from those.
    # Invalidate the location only when the number of files or their names or the
    # number of rows they have changes. If you just update a few rows, there's no
    # need to invalidate the location.
    # Invalidate the sample only when the data has drifted enough. So if you update
    # a row to have an outlier or if you add a whole bunch of new rows, make sure
    # to udpate the sample.

    # TODO: Store the time it took to collect a sample so that the user is
    # warned when invalidating a sample that took a long time to collect

    # TODO: Document the caching behavior better
    # Read location from cache. The location will include metadata like the
    # number of rows in each file as well as a sample that can be used on the
    # client side for estimating memory usage and data skew among other things.
    # Get paths with cached locations and samples
    locationspath = joinpath(homedir(), ".banyan", "sources")
    samplespath = joinpath(homedir(), ".banyan", "samples")
    locationpath = joinpath(locationspath, p |> hash |> string)
    samplepath = joinpath(samplespath, p |> hash |> string)

    # Get cached sample if it exists
    remote_sample = if isfile(samplepath) && !sample_invalid
        deserialize(samplepath)
    else
        nothing
    end

    # Get cached location if it exists
    remote_source = if isfile(locationpath) && !source_invalid
        deserialize(locationpath)
    else
        nothing
    end
    remote_source = get_remote_source_cached(get_remote_source, p; remote_source, remote_sample, shuffled=shuffled)
    remote_sample = remote_source.sample

    # Store location in cache. The same logic below applies to having a
    # `&& isempty(remote_source.files)` which effectively allows us
    # to reuse the location (computed files and row lengths) but only if the
    # location was actually already written to. If this is the first time we
    # are calling `write_parquet` with `invalidate_source=false`, then
    # the location will not be saved. But on future writes, the first write's
    # location will be used.
    if !invalidate_source && remote_source.src_name == "Remote" && remote_source.nbytes > 0
        mkpath(locationspath)
        serialize(locationpath, remote_source)
    else
        rm(locationpath, force=true, recursive=true)
    end

    # Store sample in cache. We don't store null samples because they are
    # either samples for locations that don't exist yet (write-only) or are
    # really cheap to collect the sample. Yes, it is true that generally we
    # will invalidate the sample on reads but for performance reasons someone
    # might not. But if they don't invalidate the sample, we only want to reuse
    # the sample if it was for a location that was actually written to.
    if !invalidate_sample && !isnothing(remote_sample.value)
        mkpath(samplespath)
        serialize(samplepath, remote_sample)
    else
        rm(samplepath, force=true, recursive=true)
    end

    remote_source
end

function RemoteDestination(get_remote_destination, p; invalidate_source = true, invalidate_sample = true)
    if invalidate_source
        rm(joinpath(homedir(), ".banyan", "sources", p |> hash |> string), force=true, recursive=true)
    end
    if invalidate_sample
        rm(joinpath(homedir(), ".banyan", "samples", p |> hash |> string), force=true, recursive=true)
    end

    get_remote_destination(p)
end

function get_remote_source_cached(get_remote_source, remotepath; remote_source=nothing, remote_sample=nothing, shuffled=false)::Location
    # If both the location and sample are already cached, just return them
    if !isnothing(remote_source) && !isnothing(remote_sample)
        remote_source.sample = remote_sample
        return remote_source
    end

    # This is so that we can make sure that any random selection fo rows is
    # deterministic. Might not be needed...
    Random.seed!(hash(get_session_id()))

    get_remote_source(remotepath, remote_source, remote_sample, shuffled)
end

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

function convert_to_unpooled(A)
    type_name = typeof(A).name.name
    if type_name == :PooledArray
        collect(A)
    elseif type_name == :CategoricalArray
        unwrap.(A)
    else
        # For handling SentinelArrays.MissingVector
        Base.convert(Array, A)
    end
end