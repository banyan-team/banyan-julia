#################
# Location type #
#################

const NOTHING_LOCATION = Location("None", "None", LocationParameters(), LocationParameters(), Int64(-1), NOTHING_SAMPLE, false, false)

const INVALID_LOCATION = Location("None", "None", LocationParameters(), LocationParameters(), Int64(-1), NOTHING_SAMPLE, true, true)

Location(name::String, parameters::LocationParameters, sample_memory_usage::Int64 = -1, sample::Sample = Sample())::Location =
    Location(name, name, parameters, parameters, sample_memory_usage, sample, false, false)

Base.isnothing(l::Location) = isnothing(l.sample)

LocationSource(name::String, parameters::Union{Dict{String,Any},Dict{String,String}}, sample_memory_usage::Int64 = -1, sample::Sample = Sample())::Location =
    Location(name, "None", parameters, LocationParameters(), sample_memory_usage, sample, false, false)

LocationDestination(
    name::String,
    parameters::Union{Dict{String,Any},Dict{String,String}}
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
        "sample_memory_usage" => lt.sample_memory_usage == -1 ? nothing : lt.sample_memory_usage,
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
                loc.sample_memory_usage,
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
                loc.sample_memory_usage,
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
                fut_location.sample_memory_usage,
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
                fut_location.sample_memory_usage,
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
    LocationSource("Value", Dict{String,Any}("value" => to_jl_value(val)), sample_memory_usage(val), ExactSample(val))
end

# TODO: Implement Size
Size(val)::Location = LocationSource(
    "Value",
    Dict{String,Any}("value" => to_jl_value(val)),
    0,
    Sample(indexapply(getsamplenrows, val, 1), 1),
)

function Client(val::T)::Location where {T}
    LocationSource("Client", Dict{String,Any}(), sample_memory_usage(val), ExactSample(val))
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

getsamplenrows(totalnrows::Int64)::Int64 = begin
        sc = get_sampling_config()
        # Must have at least 1 row
        cld(totalnrows, sc.always_exact ? 1 : sc.rate)
    end

# We maintain a cache of locations and a cache of samples. Locations contain
# information about what files are in the dataset and how many rows they have
# while samples contain an actual sample from that dataset

# The invalidate_* and invalidate_all_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.

function invalidate_metadata(p; kwargs...)
    lp = LocationPath(p; kwargs...)

    # Delete locally
    p = joinpath(homedir(), ".banyan", "metadata", get_metadata_path(lp))
    if isfile(p)
        rm(p)
    end

    # Delete from S3
    println("Deleting get_metadata_path(lp)=$(get_metadata_path(lp))")
    s3p = S3Path("s3://$(banyan_metadata_bucket_name())/$(get_metadata_path(lp))")
    if isfile(s3p)
        rm(s3p)
    end
end
function invalidate_samples(p; kwargs...)
    lp = LocationPath(p; kwargs...)

    # Delete locally
    samples_local_dir = joinpath(homedir(), ".banyan", "samples")
    sample_path_prefix = get_sample_path_prefix(lp)
    if isdir(samples_local_dir)
        for local_sample_path in readdir(samples_local_dir, join=true)
            if startswith(local_sample_path, sample_path_prefix)
                rm(local_sample_path)
            end
        end
    end

    # Delete from S3
    s3p = S3Path("s3://$(banyan_samples_bucket_name())/$sample_path_prefix")
    @show readdir_no_error(s3p)
    @show s3p
    @show path_as_dir(s3p)
    @show readdir(S3Path("s3://$(banyan_samples_bucket_name())"))
    if !isempty(readdir_no_error(s3p))
        rm(path_as_dir(s3p), recursive=true)
    end
    @show readdir_no_error(s3p)
    @show s3p
    @show readdir(S3Path("s3://$(banyan_samples_bucket_name())"))
end
function invalidate_location(p; kwargs...)
    invalidate_metadata(p; kwargs...)
    invalidate_samples(p; kwargs...)
end
function partition(series, partition_size)
    (series[i:min(i+(partition_size-1),end)] for i in 1:partition_size:length(series))
end
function invalidate_all_locations()
    for local_dir in [get_samples_local_path(), get_metadata_local_path()]
        rm(local_dir; force=true, recursive=true)
    end

    # Delete from S3
    for bucket_name in [banyan_samples_bucket_name(), banyan_metadata_bucket_name()]
        s3p = S3Path("s3://$bucket_name")
        if isdir_no_error(s3p)
            for p in readdir(s3p, join=true)
                rm(p, force=true, recursive=true)
            end
        end
    end 
end

function invalidate(p; after=false, kwargs...)
    if get(kwargs, after ? :invalidate_all_locations : :all_locations_invalid, false)
        invalidate_all_location()
    elseif get(kwargs, after ? :invalidate_location : :location_invalid, false)
        invalidate_location(p; kwargs...)
    elseif get(kwargs, after ? :invalidate_metadata : :metadata_invalid, false)
        invalidate_metadata(p; kwargs...)
    elseif get(kwargs, after ? :invalidate_samples : :samples_invalid, false)
        invalidate_samples(p; kwargs...)
    end
end

@specialize

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
    let banyan_samples_object_dir = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b/15117355623592221474_jl_1.8.0-beta3")
        println("Before get_location_source with readdir_no_error(banyan_samples_object_dir)=$(readdir_no_error(banyan_samples_object_dir)) and loc.metadata_invalid=$(loc.metadata_invalid) and loc.sample_invalid=$(loc.sample_invalid)")
    end
    @show lp
    @show get_sampling_configs()
    @show local_sample_path
    @show loc

    res = if !loc.metadata_invalid && !loc.sample_invalid
        # Case where both sample and parameters are valid
        loc.sample.value = load_sample(local_sample_path)
        loc.sample.rate = parse_sample_rate(local_sample_path)
        loc
    elseif loc.metadata_invalid && !loc.sample_invalid
        # Case where parameters are invalid
        let banyan_samples_object_dir = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b/15117355623592221474_jl_1.8.0-beta3")
            println("Before offloaded with readdir_no_error(banyan_samples_object_dir)=$(readdir_no_error(banyan_samples_object_dir))")
        end
        let banyan_samples_bucket = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b")
            println("Before offloaded with readdir_no_error(banyan_samples_bucket)=$(readdir_no_error(banyan_samples_bucket))")
        end
        new_loc = offloaded(_remote_source, lp, loc, args...; distributed=true)
        let banyan_samples_object_dir = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b/15117355623592221474_jl_1.8.0-beta3")
            println("After offloaded with readdir_no_error(banyan_samples_object_dir)=$(readdir_no_error(banyan_samples_object_dir))")
        end
        let banyan_samples_bucket = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b")
            println("After offloaded with readdir_no_error(banyan_samples_bucket)=$(readdir_no_error(banyan_samples_bucket))")
        end
        Arrow.write(local_metadata_path, Arrow.Table(); metadata=new_loc.src_parameters)
        @show new_loc
        new_loc.sample.value = load_sample(local_sample_path)
        new_loc
    else
        # Case where sample is invalid

        # Get the Location with up-to-date metadata (source parameters) and sample
        new_loc = offloaded(_remote_source, lp, loc, args...; distributed=true)
        @show new_loc

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
    let banyan_samples_object_dir = S3Path("s3://banyan-samples-75c0f7151604587a83055278b28db83b/15117355623592221474_jl_1.8.0-beta3")
        println("At end of RemoteSource with readdir_no_error(banyan_samples_object_dir)=$(readdir_no_error(banyan_samples_object_dir))")
    end
    res
end