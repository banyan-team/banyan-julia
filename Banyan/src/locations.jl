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
    sample::Sample

    function Location(
        src_name::String,
        dst_name::String,
        src_parameters::Dict{String,<:Any},
        dst_parameters::Dict{String,<:Any},
        sample::Sample = Sample(),
    )
        # NOTE: A file might be None and None if it is simply to be cached on
        # disk and then read from
        # if src_name == "None" && dst_name == "None"
        #     error(
        #         "Location must either be usable as a source or as a destination for data",
        #     )
        # end

        new(src_name, dst_name, src_parameters, dst_parameters, sample)
    end
end

Location(name::String, parameters::Dict{String,<:Any}, sample::Sample = Sample()) =
    Location(name, name, parameters, parameters, sample)

LocationSource(name::String, parameters::Dict{String,<:Any}, sample::Sample = Sample()) =
    Location(name, "None", parameters, LocationParameters(), sample)

LocationDestination(
    name::String,
    parameters::Dict{String,<:Any}
) = Location("None", name, LocationParameters(), parameters)

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
        # this job
        # TODO: Instead of computing the total memory usage here, compute it
        # at the end of each `@partitioned`. That way we will count twice for
        # mutation
        "total_memory_usage" => sample(lt.sample, :memory_usage) * sample(lt.sample, :rate),
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
    job = get_job()
    fut = convert(Future, fut)
    value_id = fut.value_id

    if location.src_name == "Client" || location.dst_name == "Client"
        job.futures_on_client[value_id] = fut
    else
        # TODO: Set loc of all Futures with Client loc to None at end of
        # evaluate and ensure that this is proper way to handle Client
        delete!(job.futures_on_client, value_id)
    end

    job.locations[value_id] = location
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

Value(val) = LocationSource("Value", Dict("value" => to_jl_value(val)), ExactSample(val))

# TODO: Implement Size
Size(val) = LocationSource(
    "Value",
    Dict("value" => to_jl_value(val)),
    Sample(indexapply(getsamplenrows, val, index = 1)),
)

Client(val) = LocationSource("Client", Dict{String,Any}(), ExactSample(val))
Client() = LocationDestination("Client", Dict{String,Any}())
# TODO: Un-comment only if Size is needed
# Size(size) = Value(size)

None() = Location("None", Dict{String,Any}(), Sample())
Disk() = None() # The scheduler intelligently determines when to split from and merge to disk even when no location is specified
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
# where one is the job's set sampling rate and the other has a sampling rate
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
        cld(totalnrows, get_job().sample_rate)
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

function RemoteSource(p; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)
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
    remote_source = get_remote_source(p, remote_source, remote_sample, shuffled=shuffled)
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

function RemoteDestination(p; invalidate_source = true, invalidate_sample = true)
    if invalidate_source
        rm(joinpath(homedir(), ".banyan", "sources", p |> hash |> string), force=true, recursive=true)
    end
    if invalidate_sample
        rm(joinpath(homedir(), ".banyan", "samples", p |> hash |> string), force=true, recursive=true)
    end

    get_remote_destination(p)
end

function extract_dataset_path(remotepath)
    # Detect whether this is an HDF5 file
    hdf5_ending = if occursin(".h5", remotepath)
        ".h5"
    elseif occursin(".hdf5", remotepath)
        ".hdf5"
    else
        ""
    end
    isa_hdf5 = hdf5_ending != ""

    # Get the actual path by removing the dataset from the path
    remotepath, datasetpath = if hdf5_ending == ""
        remotepath, nothing
    else
        remotepath, datasetpath = split(remotepath, hdf5_ending)
        remotepath *= hdf5_ending # Add back the file extension
        datasetpath = datasetpath[2:end] # Chop off the /
        # NOTE: It's critical that we convert `datasetpath` from a SubString
        # to a String because then the `haspath` on an `HDF5.File` will fail
        remotepath, String(datasetpath)
    end

    remotepath, datasetpath, isa_hdf5
end

function get_remote_source(remotepath, remote_source=nothing, remote_sample=nothing; shuffled=false)::Location
    # If both the location and sample are already cached, just return them
    if !isnothing(remote_source) && !isnothing(remote_sample)
        remote_source.sample = remote_sample
        return remote_source
    end

    # This is so that we can make sure that any random selection fo rows is
    # deterministic. Might not be needed...
    Random.seed!(hash(get_job_id()))

    # Handle HDF5 paths (which include the dataset in the path)
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)
    
    # Return either an HDF5 location or a table location
    if isa_hdf5
        get_remote_hdf5_source(remotepath, datasetpath, remote_source, remote_sample; shuffled=shuffled)
    else
        get_remote_table_source(remotepath, remote_source, remote_sample; shuffled=shuffled)
    end
end

function get_remote_destination(remotepath)::Location
    # Handle HDF5 paths (which include the dataset in the path)
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)

    # Return either an HDF5 location or a table location
    if isa_hdf5
        get_remote_hdf5_destination(remotepath, datasetpath)
    else
        get_remote_table_destination(remotepath)
    end
end

function convert_to_unpooled(A)
    type_name = typeof(A).name.name
    if type_name == :PooledArray
        collect(A)
    elseif type_name == :CategoricalArray
        unwrap.(A)
    else
        convert(Array, A)
    end
end
    

function get_remote_hdf5_source(remotepath, datasetpath, remote_source=nothing, remote_sample=nothing; shuffled=false)::Location
    # TODO: Cache stuff
    p = download_remote_path(remotepath)

    # TODO: Support more cases beyond just single files and all files in
    # given directory (e.g., wildcards)

    # TODO: Read cached sample if possible

    nbytes = 0
    totalnrows = 0

    # Handle single-file nd-arrays

    # TODO: Support HDF5 files that don't have .h5 in their filenmae
    # filename, datasetpath = split(p, hdf5_ending)
    # remotefilename, _ = split(remotepath, hdf5_ending)
    # filename *= hdf5_ending
    # remotefilename *= hdf5_ending
    # datasetpath = datasetpath[2:end] # Chop off the /

    # Load metadata for reading

    # TODO: Determine why sample size is so huge
    # TODO: Determine why location parameters are not getting populated

    # Open HDF5 file
    dset_sample = nothing
    datasize = nothing
    datandims = nothing
    dataeltype = nothing
    dataset_to_read_from_exists = false
    if isfile(p)
        with_downloaded_path_for_reading(p) do pp
            f = h5open(pp, "r")
            if haskey(f, datasetpath)
                dataset_to_read_from_exists = true

                dset = f[datasetpath]
                ismapping = false
                if HDF5.ismmappable(dset)
                    ismapping = true
                    dset = HDF5.readmmap(dset)
                    close(f)
                end

                # Collect metadata
                nbytes += length(dset) * sizeof(eltype(dset))
                datasize = size(dset)
                datalength = first(datasize)
                datandims = ndims(dset)
                dataeltype = eltype(dset)

                # TODO: Warn here if the data is too large
                # TODO: Modify the alert that is given before sample collection starts
                # TODO: Optimize utils_pfs.jl and generated code

                memory_used_in_sampling = datalength == 0 ? 0 : (nbytes * getsamplenrows(datalength) / datalength)
                free_memory = Sys.free_memory()
                if memory_used_in_sampling > cld(free_memory, 4)
                    @warn "Sample of $remotepath is too large (up to $(format_bytes(memory_used_in_sampling))/$(format_bytes(free_memory)) to be used). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                    GC.gc()
                end

                if isnothing(remote_sample)
                    # Collect sample
                    totalnrows = datalength
                    remainingcolons = repeat([:], ndims(dset) - 1)
                    # Start of with an empty array. The dataset has to have at
                    # least one row so we read that in and then take no data.
                    # dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
                    # If the data is already shuffled or if we just want to
                    # take an exact sample, we don't need to randomly sample here.
                    if datalength > get_max_exact_sample_length() && !shuffled
                         sampleindices = randsubseq(1:datalength, 1 / get_job().sample_rate)
                        # sample = dset[sampleindices, remainingcolons...]
                        if !isempty(sampleindices)
                            dset_sample = vcat([dset[sampleindex, remainingcolons...] for sampleindex in sampleindices]...)
                        end
                    end
                    
                    # Ensure that we have at least an empty initial array
                    if isnothing(dset_sample)
                        # NOTE: HDF5.jl does not support taking an empty slice
                        # so we have to read in the first row and then take a
                        # slice and this assumes that HDF5 datasets are always
                        # non-empty (which I think they always are).
                        dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
                    end

                    # Extend or chop sample as needed
                    samplelength = getsamplenrows(datalength)
                    # TODO: Warn about the sample size being too large
                    if size(dset_sample, 1) < samplelength
                        dset_sample = vcat(
                            dset_sample,
                            dset[1:(samplelength-size(dset_sample, 1)), remainingcolons...],
                        )
                    else
                        dset_sample = dset[1:samplelength, remainingcolons...]
                    end
                end

                # Close HDF5 file
                if !ismapping
                    close(f)
                end
            end
        end
    end

    # If the sample is a PooledArray or CategoricalArray, convert it to a
    # simple array so we can correctly compute its memory usage.
    if !isnothing(dset_sample) && !(dset_sample isa Array)
        dset_sample = convert_to_unpooled(dset_sample)
    end

    loc_for_reading, metadata_for_reading = if dataset_to_read_from_exists
        (
            "Remote",
            Dict(
                "path" => remotepath,
                "subpath" => datasetpath,
                "size" => datasize,
                "ndims" => datandims,
                "eltype" => dataeltype,
                "nbytes" => 0
            ),
        )
    else
        ("None", Dict{String,Any}())
    end

    # Get the remote sample
    if isnothing(remote_sample)
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        elseif totalnrows <= get_max_exact_sample_length()
            ExactSample(dset_sample, total_memory_usage = nbytes)
        else
            Sample(dset_sample, total_memory_usage = nbytes)
        end
    end

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        remote_sample,
    )
end

function get_remote_hdf5_destination(remotepath, datasetpath)
    # Load metadata for writing to HDF5 file
    loc_for_writing, metadata_for_writing =
        ("Remote", Dict("path" => remotepath, "subpath" => datasetpath, "nbytes" => 0))

    LocationDestination(
        loc_for_writing,
        metadata_for_writing
    )
end

# TODO: Add ``; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false``

function RemoteONNXSource(remotepath)::Location
    # RemoteSource(
    #     remotepath,
    #     shuffled=shuffled,
    #     source_invalid = source_invalid,
    #     sample_invalid,
    #     invalidate_source = invalidate_source,
    #     invalidate_sample = invalidate_sample
    # ) do remotepath, remote_source, remote_sample, shuffled

    # Get the path
    p = download_remote_path(remotepath)
    p_exists = isfile(p)

    # isa_onnx = endswith(p, ".onnx")
    # if !isa_onnx
    #     error("Expected ONNX file for $remotepath")
    # end

    loc_for_reading, metadata_for_reading = if p_exists
        (
            # TODO: Change back to Remote and then specify locations to specify that format parameter is important in dispatching in the
            # PF dispatch table entry
            "RemoteONNX",
            Dict(
                "path" => remotepath,
                "format" => "onnx"
            ),
        )
    else
        ("None", Dict{String,Any}())
    end

    # Construct sample
    remote_sample = if isnothing(loc_for_reading)
        Sample()
    else
        s = nothing
        with_downloaded_path_for_reading(p) do pp
            s = ExactSample(load_inference(pp))
        end
        s
    end

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        remote_sample,
    )

    # end
end


function get_image_format(path)
    if endswith(path, ".png")
        "png"
    elseif endswith(path, ".jpg")
        "jpg"
    else
        error("Unsupported file format. Must be jpg or png")
    end
end

function RemoteImageSource(remotepath, remote_source=nothing, remote_sample=nothing; shuffled=false)::Location

    # TODO: Add caching
    # # Initialize parameters if location is already cached
    # files = isnothing(remote_source) ? [] : remote_source.files  # list, serialized generator
    # nimages = isnothing(remote_source) ? 0 : remote_source.nimages
    # nbytes = isnothing(remote_source) ? 0 : remote_source.nbytes
    # ndims = isnothing(remote_source) ? 0 : remote_source.ndims
    # datasize = isnothing(remote_source) ? () : remote_source.size
    # dataeltype = isnothing(remote_source) ? "" : remote_source.eltype
    # format = isnothing(remote_source) ? "" : remote_source.format  # png, jpg
    files = nothing
    nimages = nothing
    nbytes = nothing
    ndims = nothing
    datasize = nothing
    dataeltype = nothing
    format = nothing

    # TODO: I think if the above parameters were cached, they still get
    # read in again

    # Remote path is either a single file path, a list of file paths,
    # or a generator. The file paths can either be S3 or HTTP
    if isa(remotepath, Base.Generator)
        files_to_read_from = remotepath
    else

        if !isa(remotepath, Base.Array)
            p = Banyan.download_remote_path(remotepath)

            # Determine if this is a directory
            p_isfile = isfile(p)
            newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
            p_isdir = !p_isfile && isdir(newp_if_isdir)
            if p_isdir
                p = newp_if_isdir
            end

            # Get files to read
            files_to_read_from = if p_isdir
                [joinpath(remotepath, filep) for filep in Random.shuffle(readdir(p))]
            elseif p_isfile
                [remotepath]
            else
                []
            end
        else
            files_to_read_from = remotepath
        end
    end

    # Determine nimages
    if isnothing(remote_source)
        iterator_size = Iterators.IteratorSize(files_to_read_from)
        if iterator_size == Base.IsInfinite()
            error("Infinite generators are not supported")
        elseif iterator_size == Base.SizeUnknown()
            nimages = sum(1 for _ in files_to_read_from)
        else  # length can be predetermined
            nimages = length(files_to_read_from)
        end
    end
    meta_collected = false

    # Initialize sample
    randomsample = nothing

    println("HEREHEHRE: ", isnothing(remote_sample), isnothing(remote_source))

    if isnothing(remote_sample)

        samplesize = Banyan.get_max_exact_sample_length()
        nbytes_of_sample = 0

        progressbar = Progress(length(files_to_read_from), "Collecting sample from $remotepath")
        for filep in files_to_read_from
            println("next file")
            p = download_remote_path(filep)
            with_downloaded_path_for_reading(p) do pp

                # Load file and collect metadata and sample
                image = load(pp)

                if isnothing(remote_source) && !meta_collected
                    println("collecting meta 1")
                    nbytes = length(image) * sizeof(eltype(image)) * nimages
                    ndims = length(size(image)) + 1 # first dim
                    dataeltype = eltype(image)
                    datasize = (nimages, size(image)...)
                    format = get_image_format(pp)
                    meta_collected = true
                end
                nbytes_of_sample += length(image) * sizeof(eltype(image))

                if isnothing(randomsample)
                    randomsample = []
                end
                if length(randomsample) < samplesize
                    push!(randomsample, reshape(image, (1, size(image)...)))  # add first dimension
                end

                # TODO: Warn about sample being too large

            end

            # Stop as soon as we get our sample
            if (!isnothing(randomsample) && size(randomsample)[1] == samplesize) || samplesize == 0
                break
            end

            next!(progressbar)
        end
        finish!(progressbar)

        if isnothing(remote_source)
            # Estimate nbytes based on the sample
            nbytes = (nbytes_of_sample / length(randomsample)) * length(files_to_read_from)
        end

    elseif isnothing(remote_source)
        # No location, but has sample
        # In this case, read one random file to collect metadata
        # We assume that all files have the same nbytes and ndims

        filep = collect(Iterators.take(Iterators.reverse(files_to_read_from), 1))[1]
        p = download_remote_path(filep)
        with_downloaded_path_for_reading(p) do pp
            println("collecting meta here 2")

            # Load file and collect metadata and sample
            image = load(pp)

            nbytes = length(image) * sizeof(eltype(image)) * nimages
            ndims = length(size(image)) + 1 # first dim
            dataeltype = eltype(image)
            datasize = (nimages, size(image)...)
            format = get_image_format(pp)
        end
    end

    # Serialize generator
    if isnothing(remote_source)
        files = isa(files_to_read_from, Base.Generator) ? Banyan.to_jl_value_contents(files_to_read_from) : files_to_read_from
    end

    loc_for_reading, metadata_for_reading = if !isnothing(files) && !isempty(files)
        (
            # TODO: Change this back to Remote and then have locations in the PF dispatch table entry require the format to be Image
            "RemoteImage",
            Dict(
                "path" => remotepath,
                "files" => files,  # either a serialized generator or list of filepaths
                "nimages" => nimages,
                "nbytes" => nbytes,  # assume all files have same size
                "ndims" => ndims,
                "size" => datasize,
                "eltype" => dataeltype,
                "format" => format
            ),
        )
    else
        ("None", Dict{String,Any}())
    end

    # Get the remote sample
    if isnothing(remote_sample)
        randomsample = cat(randomsample..., dims=1) # Put to correct shape
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        elseif length(files) <= Banyan.get_max_exact_sample_length()
            ExactSample(randomsample)
        else
            Sample(randomsample)
        end
    end

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        remote_sample,
    )
end

get_csv_chunks(localfilepathp) = 
    try
        CSV.Chunks(localfilepathp)
    catch e
        # An ArgumentError may get thrown if the file cannot be
        # read in with the multi-threaded chunked iterator for
        # some reason. See
        # https://github.com/JuliaData/CSV.jl/blob/main/src/context.jl#L583-L641
        # for possible reasons for `ctx.threaded` in CSV.jl
        # code to be false.
        if isa(e, ArgumentError)
            [CSV.File(localfilepathp)]
        else
            throw(e)
        end
    end

function get_remote_table_source(remotepath, remote_source=nothing, remote_sample=nothing; shuffled=false)::Location
    # `remote_source` and `remote_sample` might be non-null indicating that
    # we need to reuse a location or a sample (but not both since then the
    # `Remote` constructor would have just returned the location).
    # 
    # `shuffled` only applies to sample collection. If we
    # have to collect a sample, then `shuffled` allows us to only read in the
    # first few files. Shuffled basically implies that either the data is
    # shuffled already or the files are similar enough in their file-specific
    # data distribution that we can just randomly select files and read them in
    # sequentially until we have enough for the sample.

    # Get the path ready for using for collecting sample or location info or both
    p = download_remote_path(remotepath)

    # Initialize location parameters with the case that we already have a
    # location
    files = isnothing(remote_source) ? [] : remote_source.files
    totalnrows = isnothing(remote_source) ? 0 : remote_source.nrows
    nbytes = isnothing(remote_source) ? 0 : remote_source.nbytes

    # Determine whether this is a directory
    p_isfile = isfile(p)
    newp_if_isdir = endswith(string(p), "/") ? p : (p * "/")
    # AWSS3.jl's S3Path will return true for isdir as long as it ends in
    # a / and will then return empty for readdir. When using S3FS, isdir will
    # actually return false if it isn't a directory but if you call readdir it
    # will fail.
    p_isdir = !p_isfile && isdir(newp_if_isdir)
    if p_isdir
        p = newp_if_isdir
    end

    # Get files to read
    files_to_read_from = if p_isdir
        Random.shuffle(readdir(p))
    elseif p_isfile
        [p]
    else
        []
    end

    # The possibilities:
    # - No location but has sample
    # - No sample but has location, shuffled
    # - No sample, no location, shuffled
    # - No sample but has location, not shuffled
    # - No sample, no location, not shuffled

    # Initialize sample
    exactsample = nothing
    randomsample = nothing
    emptysample = nothing
    already_warned_about_too_large_sample = false
    memory_used_in_sampling = 0

    # A second pass is only needed if there is no sample and the data is
    # shuffled. On this second pass, we only read in some files.

    # Read in location info if we don't have. We also collect the sample along
    # the way unless the data is shuffled - then we wait till after we have a
    # location to read in the location.
    
    # Loop through files; stop early if we don't need 

    progressbar = Progress(length(files_to_read_from), isnothing(remote_sample) ? "Collecting sample from $remotepath" : "Collecting location information from $remotepath")
    for (fileidx, filep) in enumerate(files_to_read_from)
        # Initialize
        filenrows = 0

        # We download the file because either we need to get the location or
        # we need to collect a sample. If we already have a location and are
        localfilepath = p_isdir ? joinpath(p, filep) : p
        with_downloaded_path_for_reading(localfilepath) do localfilepathp
            # If the data is shuffled, we don't read it it in until we know how
            # many rows there are. We only collect a sample now if we don't
            # already have one and the data isn't shuffled. Because if we
            # don't have a sample and the data _is_ shuffled, we want to
            # compute the sample later.
            if isnothing(remote_sample) && !shuffled
                chunks = if endswith(localfilepathp, ".csv")
                    get_csv_chunks(localfilepathp)
                elseif endswith(localfilepathp, ".parquet")
                    Tables.partitions(read_parquet(localfilepathp))
                elseif endswith(localfilepathp, ".arrow")
                    Arrow.Stream(localfilepathp)
                else
                    error("Expected .csv or .parquet or .arrow")
                end

                # Sample from each chunk
                for (i, chunk) in enumerate(chunks)
                    chunkdf = DataFrames.DataFrame(chunk, copycols=false)
                    chunknrows = nrow(chunkdf)
                    filenrows += chunknrows
                    if isnothing(remote_source)
                        totalnrows += chunknrows
                    end

                    # Note that we only initialize and/or append to the samples
                    # if this chunk isn't empty. The chunk could be empty and
                    # in the case of CSV.jl, the columns would just be missing
                    # vectors and we wouldn't be able to append to that. So
                    # instead we wait till we get something that we cam
                    # actually use as a sample.

                    # Note that one potential issue is that we might
                    # incorrectly infer schema by assuming that the first
                    # non-empty partition has the schema of the whole data
                    # frame.

                    # Use `chunkdf` to initialize the schema of the sampels
                    # regardless of whethere `chunkdf` has any rows or not.
                    if isnothing(randomsample) && !isempty(chunkdf)
                        randomsample = empty(chunkdf)
                    end
                    if isnothing(exactsample) && !isempty(chunkdf)
                        exactsample = empty(chunkdf)
                    end
                    if isnothing(emptysample)
                        emptysample = empty(chunkdf)
                    end

                    # Append to randomsample
                    # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
                    chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
                    # if any(chunksampleindices)
                    if !isempty(chunkdf) && !isempty(chunksampleindices)
                        append!(randomsample, @view chunkdf[chunksampleindices, :])
                    end

                    # Append to exactsample
                    samplenrows = getsamplenrows(totalnrows)
                    if !isempty(chunkdf) && nrow(exactsample) < samplenrows
                        append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
                    end

                    # Warn about sample being too large
                    # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                    memory_used_in_sampling += total_memory_usage(chunkdf)
                    memory_used_in_sampling_total = memory_used_in_sampling + total_memory_usage(exactsample) + total_memory_usage(randomsample)
                    chunkdf = nothing
                    chunk = nothing
                    free_memory = Sys.free_memory()
                    if memory_used_in_sampling_total > cld(free_memory, 4)
                        if !already_warned_about_too_large_sample
                            @warn "Sample of $remotepath is too large ($(format_bytes(memory_used_in_sampling_total))/$(format_bytes(free_memory)) used so far). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                            already_warned_about_too_large_sample = true
                        end
                        GC.gc()
                    end
                end
            elseif isnothing(remote_source)
                # Even if the data is shuffled, we will collect location
                # information that can subsequently be used in the second
                # pass to read in files until we have a sample.

                filenrows = if endswith(localfilepathp, ".csv")
                    sum((1 for row in CSV.Rows(localfilepathp)))
                elseif endswith(localfilepathp, ".parquet")
                    nrows(Parquet.File(localfilepathp))
                elseif endswith(localfilepathp, ".arrow")
                    Tables.rowcount(Arrow.Table(localfilepathp))
                else
                    error("Expected .csv or .parquet or .arrow")
                end
                totalnrows += filenrows

                # TODO: Update nbytes if there is no existing location and
                # we need nbytes for the location even though we don't need
                # to collect a sample.+++

                # TODO: Maybe also compute nbytes or perhaps it's okay to just
                # use the sample to estimate the total memory usage
            end

            # Add to list of file metadata
            if isnothing(remote_source)
                push!(
                    files,
                    Dict(
                        "path" => p_isdir ? joinpath(remotepath, filep) : remotepath,
                        "nrows" => filenrows,
                    ),
                )
            end
        end

        next!(progressbar)
    end
    finish!(progressbar)

    # A second (partial) pass over the data if we don't yet have a sample and
    # the data is shuffled. If we didn't have a sample but the data wasn't
    # shuffled we would have collected it already in the first pass.
    if isnothing(remote_sample) && shuffled
        # We should now know exactly how many rows there are and how many
        # to sample.
        samplenrows = getsamplenrows(totalnrows)

        # So we can iterate through the files (reverse in case some caching helps
        # us). We append to `randomsample` directly.
        progressbar = Progress(length(files_to_read_from), "Collecting sample from $remotepath")
        for filep in reverse(files_to_read_from)
            localfilepath = p_isdir ? joinpath(p, filep) : p
            with_downloaded_path_for_reading(localfilepath) do localfilepathp
                chunks = if endswith(localfilepathp, ".csv")
                    get_csv_chunks(localfilepathp)
                elseif endswith(localfilepathp, ".parquet")
                    Tables.partitions(read_parquet(localfilepathp))
                elseif endswith(localfilepathp, ".arrow")
                    Arrow.Stream(localfilepathp)
                else
                    error("Expected .csv or .parquet or .arrow")
                end

                # Sample from each chunk
                for (i, chunk) in enumerate(chunks)
                    # Read in chunk
                    chunkdf = DataFrames.DataFrame(chunk, copycols=false)

                    # Use `chunkdf` to initialize the schema of the sampels
                    # regardless of whethere `chunkdf` has any rows or not.
                    if !isempty(chunkdf) && isnothing(randomsample)
                        randomsample = empty(chunkdf)
                    end
                    if !isempty(chunkdf) && isnothing(exactsample)
                        exactsample = empty(chunkdf)
                    end
                    if isnothing(emptysample)
                        emptysample = empty(chunkdf)
                    end

                    # Append to randomsample; append to exactsample later
                    if !isempty(chunkdf) && nrow(randomsample) < samplenrows
                        append!(randomsample, first(chunkdf, samplenrows - nrow(randomsample)))
                    end

                    # Warn about sample being too large
                    # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                    memory_used_in_sampling += total_memory_usage(chunkdf)
                    memory_used_in_sampling_total = memory_used_in_sampling + 2 * total_memory_usage(randomsample)
                    chunkdf = nothing
                    chunk = nothing
                    free_memory = Sys.free_memory()
                    if memory_used_in_sampling_total > cld(free_memory, 4)
                        if !already_warned_about_too_large_sample
                            @warn "Sample of $remotepath is too large ($(format_bytes(memory_used_in_sampling_total))/$(format_bytes(free_memory)) used so far). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                            already_warned_about_too_large_sample = true
                        end
                        GC.gc()
                    end

                    # Stop as soon as we get our sample
                    if (!isnothing(randomsample) && nrow(randomsample) == samplenrows) || samplenrows == 0
                        break
                    end
                end
            end

            # Stop as soon as we get our sample. We have reached our sample if
            # there were files to build up a sample and we got how many rows
            # we wanted or the sample should have rows. Because if it should
            # have no rows, then it already had a chance to get a sample and if
            # it didn't, it will just have to settle for a schema-less
            # `DataFrame`.
            if (!isnothing(randomsample) && nrow(randomsample) == samplenrows) || samplenrows == 0
                break
            end
            
            next!(progressbar)
        end
        finish!(progressbar)

        # In this case, the random sample would also be the exact sample if an
        # exact sample is ever required.
        if totalnrows <= get_max_exact_sample_length()
            exactsample = randomsample
        end
    end

    # If there were no files, set the samples to schema-less data frames.
    if !isnothing(randomsample)
        # This is the case where some partition was non-empty and so we can
        # take that schema-ful sample and take the empty version of that.
        emptysample = empty(randomsample)
    elseif isnothing(emptysample)
        # If there were no partitions, we never would have set `emptysample`
        # and so we default to an empty schema-less data frame.
        emptysample = DataFrame()
    end
    # Hopefully there were some partitions and `emptysample` is a
    # schema-ful (having the schema of the first partition) data frame.
    if isnothing(randomsample)
        randomsample = emptysample
    end
    if isnothing(exactsample)
        exactsample = emptysample
    end

    # Adjust sample to have samplenrows
    if isnothing(remote_sample)
        samplenrows = getsamplenrows(totalnrows) # Either a subset of rows or the whole thing
        # If we already have enough rows in the exact sample...
        if totalnrows <= get_max_exact_sample_length()
            randomsample = exactsample
        end
        # Regardless, expand the random sample as needed...
        if nrow(randomsample) < samplenrows
            append!(randomsample, first(exactsample, samplenrows - nrow(randomsample)))
        end
        # ... and limit it as needed
        if nrow(randomsample) > samplenrows
            randomsample = first(randomsample, samplenrows)
        end
    end

    # If the sample is a PooledArray or CategoricalArray, convert it to a
    # simple array so we can correctly compute its memory usage.
    for s in [emptysample, randomsample]
        for pn in Base.propertynames(s)
            sc = s[!, pn]
            if !(sc isa Array)
                s[!, pn] = convert_to_unpooled(sc)
            end
        end
    end

    # Re-compute the number of bytes. Even if we are reusing a location, we go
    # ahead and re-compute the sample. Someone may have written data with
    # `invalidate_source=false` but `shuffled=true` (perhaps the only thing
    # they changed was adding a column and somehow they ensured that the same
    # data got written to the same files) and so we want to estimate the total
    # memory usage using a newly collected sample but the same `totalnrow` we
    # already know from the reused location.
    remote_sample_value = isnothing(remote_sample) ? randomsample : remote_sample.value
    remote_sample_rate = totalnrows > 0 ? totalnrows / nrow(remote_sample_value) : 1.0
    nbytes = ceil(total_memory_usage(remote_sample_value) * remote_sample_rate)

    # Load metadata for reading
    # If we're not using S3FS, the files might be empty because `readdir`
    # would just return empty but `p_isdir` might be true because S3Path will
    # say so for pretty much anything unless it's a file. So we might end up
    # creating a location source with a path that doesn't exist and an empty
    # list of files. But that's actually okay because in the backend, we will
    # check isdir before we ever try to readdir from something. We will check
    # both isfile and isdir. So for example, we won't read in an HDF5 file if
    # it is not a file. And we won't call readdir if it isn't isdir and that
    # works fine in the backend since we use S3FS there.
    loc_for_reading, metadata_for_reading = if !isempty(files) || p_isdir # empty directory can still be read from
        ("Remote", Dict("path" => remotepath, "files" => files, "nrows" => totalnrows, "nbytes" => nbytes, "emptysample" => to_jl_value_contents(empty(randomsample))))
    else
        ("None", Dict{String,Any}())
    end

    # Get remote sample
    if isnothing(remote_sample)
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        elseif totalnrows <= get_max_exact_sample_length()
            ExactSample(randomsample, total_memory_usage = nbytes)
        else
            Sample(randomsample, total_memory_usage = nbytes)
        end
    else
        # Adjust sample properties if this is a reused sample. But we only do
        # this if we are reading from this location. If we are writing some
        # value to some location that doesn't exist yet, we don't want to
        # change the sample or the sample rate.
        # TODO: Don't set the sample or sample properties if we are merely trying to overwrite something.
        setsample!(remote_sample, :memory_usage, ceil(nbytes / remote_sample_rate))
        setsample!(remote_sample, :rate, remote_sample_rate)
    end

    # In the process of collecting this sample, we have called functions in
    # `utils_s3fs.jl` for reading from remote locations. These functions save
    # files in /tmp and so we need to cleanup (delete) these files.
    # TODO: Surround this function by a try-catch so that if this fails, we
    # will cleanup anyway.
    cleanup_tmp()

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        remote_sample,
    )
end

function get_remote_table_destination(remotepath)
    # Load metadata for writing
    # NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
    # or CSV dataset is desired to be created
    loc_for_writing, metadata_for_writing = ("Remote", Dict("path" => remotepath, "files" => [], "nrows" => 0, "nbytes" => 0))

    LocationDestination(loc_for_writing, metadata_for_writing)
end