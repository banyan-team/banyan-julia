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
    parameters::Dict{String,<:Any},
    sample::Sample = Sample(),
) = Location("None", name, LocationParameters(), parameters, sample)

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
        error("$name not found in location parameters")
    end
end

function Base.hasproperty(loc::Location, name::Symbol)
    n = string(name)
    hasfield(Location, name) || haskey(loc.src_parameters, n) || haskey(loc.dst_parameters, n)
end

function to_jl(lt::Location)
    if is_debug_on()
        @show sample(lt.sample, :memory_usage)
        @show sample(lt.sample, :memory_usage) * sample(lt.sample, :rate)
        @show sample(lt.sample, :rate)
    end
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
    located(
        fut,
        Location(
            loc.src_name,
            isnothing(fut_location) ? "None" : fut_location.dst_name,
            loc.src_parameters,
            isnothing(fut_location) ? Dict{String,Any}() : fut_location.dst_parameters,
            (
                isnothing(fut_location) ||
                sample(loc.sample, :memory_usage) >
                sample(fut_location.sample, :memory_usage)
            ) ? loc.sample : fut_location.sample,
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
            (
                isnothing(fut_location) ||
                sample(loc.sample, :memory_usage) >
                sample(fut_location.sample, :memory_usage)
            ) ? loc.sample : fut_location.sample,
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
    @debug value_id
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

clear_locations() = rm(joinpath(homedir(), ".banyan", "locations"), force=true, recursive=true)
clear_samples() = rm(joinpath(homedir(), ".banyan", "samples"), force=true, recursive=true)
invalidate_location(p) = rm(joinpath(homedir(), ".banyan", "locations", p |> hash |> string), force=true, recursive=true)
invalidate_sample(p) = rm(joinpath(homedir(), ".banyan", "samples", p |> hash |> string), force=true, recursive=true)

function Remote(p; shuffled=false, location_invalid = false, sample_invalid = false, invalidate_location = false, invalidate_sample = false)
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
    locationspath = joinpath(homedir(), ".banyan", "locations")
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
    remote_location = if isfile(locationpath) && !location_invalid
        deserialize(locationpath)
    else
        nothing
    end
    remote_location = get_remote_location(p, remote_location, remote_sample, shuffled=shuffled)
    remote_sample = remote_location.sample

    # Store location in cache. The same logic below applies to having a
    # `&& isempty(remote_location.files)` which effectively allows us
    # to reuse the location (computed files and row lengths) but only if the
    # location was actually already written to. If this is the first time we
    # are calling `write_parquet` with `invalidate_location=false`, then
    # the location will not be saved. But on future writes, the first write's
    # location will be used.
    if !invalidate_location && remote_location.nbytes > 0
        mkpath(locationspath)
        serialize(locationpath, remote_location)
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

    remote_location
end

function get_remote_location(remotepath, remote_location=nothing, remote_sample=nothing; shuffled=false)::Location
    @info "Collecting sample from $remotepath\n\nThis will take some time but the sample will be cached for future use. Note that writing to this location will invalidate the cached sample."

    # If both the location and sample are already cached, just return them
    if !isnothing(remote_location) && !isnothing(remote_sample)
        remote_location.sample = remote_sample
        return remote_location
    end

    # This is so that we can make sure that any random selection fo rows is deterministic. Might not be needed
    Random.seed!(hash(get_job_id()))

    # Detect whether this is an HDF5 file
    hdf5_ending = if occursin(".h5", remotepath)
        ".h5"
    elseif occursin(".hdf5", remotepath)
        ".hdf5"
    else
        ""
    end
    isa_hdf5 = hdf5_ending != ""
    
    # Return either an HDF5 location or a table location
    if isa_hdf5
        get_remote_hdf5_location(remotepath, hdf5_ending, remote_location, remote_sample; shuffled=shuffled)
    else
        get_remote_table_location(remotepath, remote_location, remote_sample; shuffled=shuffled)
    end
end

function get_remote_hdf5_location(remotepath, hdf5_ending, remote_location=nothing, remote_sample=nothing; shuffled=false)::Location
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
                datandims = ndims(dset)
                dataeltype = eltype(dset)

                if isnothing(remote_sample)
                    # Collect sample
                    datalength = first(datasize)
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
    if is_debug_on()
        @show metadata_for_reading
    end

    # Load metadata for writing to HDF5 file
    loc_for_writing, metadata_for_writing =
        ("Remote", Dict("path" => remotepath, "subpath" => datasetpath, "nbytes" => 0))

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
    return Location(
        loc_for_reading,
        loc_for_writing,
        metadata_for_reading,
        metadata_for_writing,
        remote_sample,
    )
end

function get_remote_table_location(remotepath, remote_location=nothing, remote_sample=nothing; shuffled=false)::Location
    # `remote_location` and `remote_sample` might be non-null indicating that
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
    files = isnothing(remote_location) ? [] : remote_location.files
    totalnrows = isnothing(remote_location) ? 0 : remote_location.nrows
    nbytes = isnothing(remote_location) ? 0 : remote_location.nbytes

    # Determine whether this exists
    p_isdir = isdir(p)
    p_isfile = !p_isdir && isfile(p) # <-- avoid expensive unnecessary S3 API calls
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
    exactsample = DataFrame()
    randomsample = DataFrame()
    already_warned_about_too_large_sample = false

    # A second pass is only needed if there is no sample and the data is
    # shuffled. On this second pass, we only read in some files.

    # Read in location info if we don't have. We also collect the sample along
    # the way unless the data is shuffled - then we wait till after we have a
    # location to read in the location.
    
    # Loop through files; stop early if we don't need 
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
                    CSV.Chunks(localfilepathp)
                elseif endswith(localfilepathp, ".parquet")
                    Tables.partitions(read_parquet(localfilepathp))
                elseif endswith(localfilepathp, ".arrow")
                    Arrow.Stream(localfilepathp)
                else
                    error("Expected .csv or .parquet or .arrow")
                end

                # Sample from each chunk
                for (i, chunk) in enumerate(chunks)
                    @show i
                    chunkdf = chunk |> DataFrames.DataFrame
                    chunknrows = nrow(chunkdf)
                    filenrows += chunknrows
                    if isnothing(remote_location)
                        totalnrows += chunknrows
                    end

                    # Append to randomsample
                    # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
                    chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
                    # if any(chunksampleindices)
                    if !isempty(chunksampleindices)
                        append!(randomsample, @view chunkdf[chunksampleindices, :])
                    end

                    # Append to exactsample
                    samplenrows = getsamplenrows(totalnrows)
                    @show samplenrows
                    if nrow(exactsample) < samplenrows
                        append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
                    end

                    # Warn about sample being too large
                    # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                    memory_used_in_sampling = total_memory_usage(chunk) + total_memory_usage(chunkdf) + total_memory_usage(exactsample) + total_memory_usage(randomsample)
                    chunkdf = nothing
                    chunk = nothing
                    if memory_used_in_sampling > cld(Sys.free_memory(), 10)
                        if !already_warned_about_too_large_sample
                            @warn "Sample is too large; try creating a job with a greater `sample_rate` than the number of workers (default is 2)"
                            already_warned_about_too_large_sample = true
                        end
                        GC.gc()
                    end
                end
            elseif isnothing(remote_location)
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
            if isnothing(remote_location)
                push!(
                    files,
                    Dict(
                        "path" => p_isdir ? joinpath(remotepath, filep) : remotepath,
                        "nrows" => filenrows,
                    ),
                )
            end
        end
    end

    # A second (partial) pass over the data if we don't yet have a sample and
    # the data is shuffled. If we didn't have a sample but the data wasn't
    # shuffled we would have collected it already in the first pass.
    if isnothing(remote_sample) && shuffled
        # We should now know exactly how many rows there are and how many
        # to sample.
        samplenrows = getsamplenrows(totalnrows)

        # So we can iterate through the files (reverse in case some caching helps
        # us). We append to `randomsample` directly.
        for filep in reverse(files_to_read_from)
            localfilepath = p_isdir ? joinpath(p, filep) : p
            with_downloaded_path_for_reading(localfilepath) do localfilepathp
                chunks = if endswith(localfilepathp, ".csv")
                    CSV.Chunks(localfilepathp)
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
                    chunkdf = chunk |> DataFrames.DataFrame

                    # Append to exactsample
                    if nrow(randomsample) < samplenrows
                        append!(randomsample, first(chunkdf, samplenrows - nrow(randomsample)))
                    end

                    # Stop as soon as we get our sample
                    if nrow(randomsample) == samplenrows
                        break
                    end
                end
            end

            # Stop as soon as we get our sample
            if nrow(randomsample) == samplenrows
                break
            end
        end

        # In this case, the random sample would also be the exact sample if an
        # exact sample is ever required.
        if totalnrows <= get_max_exact_sample_length()
            exactsample = randomsample
        end
    end

    # Adjust sample to have samplenrows
    if isnothing(remote_sample)
        samplenrows = getsamplenrows(totalnrows) # Either a subset of rows or the whole thing
        if is_debug_on()
            @show samplenrows
        end
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

    # Re-compute the number of bytes. Even if we are reusing a location, we go
    # ahead and re-compute the sample. Someone may have written data with
    # `invalidate_location=false` but `shuffled=true` (perhaps the only thing
    # they changed was adding a column and somehow they ensured that the same
    # data got written to the same files) and so we want to estimate the total
    # memory usage using a newly collected sample but the same `totalnrow` we
    # already know from the reused location.
    remote_sample_value = isnothing(remote_sample) ? randomsample : remote_sample.value
    remote_sample_rate = totalnrows / nrow(remote_sample_value)
    nbytes = ceil(total_memory_usage(remote_sample_value) * remote_sample_rate)

    @show isnothing(remote_location)
    @show isnothing(remote_sample)
    @show nbytes

    # Load metadata for reading
    loc_for_reading, metadata_for_reading = if !isempty(files) || p_isdir # empty directory can still be read from
        ("Remote", Dict("path" => remotepath, "files" => files, "nrows" => totalnrows, "nbytes" => nbytes))
    else
        ("None", Dict{String,Any}())
    end

    # Load metadata for writing
    # NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
    # or CSV dataset is desired to be created
    loc_for_writing, metadata_for_writing = ("Remote", Dict("path" => remotepath, "files" => [], "nrows" => 0, "nbytes" => 0))

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
        # Adjust sample properties if this is a reused sample
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
    Location(
        loc_for_reading,
        loc_for_writing,
        metadata_for_reading,
        metadata_for_writing,
        remote_sample,
    )
end