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
    if haskey(loc.src_parameters, n)
        loc.src_parameters[n]
    elseif haskey(loc.dst_parameters, n)
        loc.dst_parameters[n]
    else
        error("$name not found in location parameters")
    end
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

######################################################
# Helper functions for serialization/deserialization #
######################################################

to_jl_value(jl) = Dict("is_banyan_value" => true, "contents" => to_jl_value_contents(jl))

# NOTE: This function is shared between the client library and the PT library
to_jl_value_contents(jl) = begin
    # Handle functions defined in a module
    # TODO: Document this special case
    # if jl isa Function && !(isdefined(Base, jl) || isdefined(Core, jl) || isdefined(Main, jl))
    if jl isa Expr && eval(jl) isa Function
        jl = Dict("is_banyan_udf" => true, "code" => jl)
    end

    # Convert Julia object to string
    io = IOBuffer()
    iob64_encode = Base64EncodePipe(io)
    serialize(iob64_encode, jl)
    close(iob64_encode)
    String(take!(io))
end

# NOTE: This function is shared between the client library and the PT library
from_jl_value_contents(jl_value_contents) = begin
    # Converty string to Julia object
    io = IOBuffer()
    iob64_decode = Base64DecodePipe(io)
    write(io, jl_value_contents)
    seekstart(io)
    res = deserialize(iob64_decode)

    # Handle functions defined in a module
    if res isa Dict && haskey(res, "is_banyan_udf") && res["is_banyan_udf"]
        eval(res["code"])
    else
        res
    end
end

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

# MAX_EXACT_SAMPLE_LENGTH = 1024
MAX_EXACT_SAMPLE_LENGTH = if is_debug_on()
    50
else
    1024
end

getsamplenrows(totalnrows) =
    if totalnrows <= MAX_EXACT_SAMPLE_LENGTH
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

function Remote(p; shuffled=false, similar_files=false, location_invalid = false, sample_invalid = false, invalidate_location = false, invalidate_sample = false)
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
        get_remote_location(p, remote_sample, shuffled=shuffled, similar_files=similar_files)
    end
    remote_sample = remote_location.sample

    # Store location in cache
    if !invalidate_location
        mkpath(locationspath)
        serialize(locationpath, remote_location)
    else
        rm(locationpath, force=true, recursive=true)
    end

    # Store sample in cache
    if !invalidate_sample
        mkpath(samplespath)
        serialize(samplepath, remote_sample)
    else
        rm(samplepath, force=true, recursive=true)
    end

    remote_location
end

function get_remote_location(remotepath, remote_sample=nothing; shuffled=false, similar_files=false)::Location
    @info "Collecting sample from $remotepath\n\nThis will take some time but the sample will be cached for future use. Note that writing to this location will invalidate the cached sample."

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
        get_remote_hdf5_location(remotepath, hdf5_ending, remote_sample; shuffled=shuffled, similar_files=similar_files)
    else
        get_remote_table_location(remotepath, remote_sample; shuffled=shuffled, similar_files=similar_files)
    end
end

function get_remote_hdf5_location(remotepath, hdf5_ending, remote_sample=nothing; shuffled=false, similar_files=false)::Location
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
                    if datalength > MAX_EXACT_SAMPLE_LENGTH || shuffled
                         sampleindices = randsubseq(1:datalength, 1 / get_job().sample_rate)
                        # sample = dset[sampleindices, remainingcolons...]
                        if !isempty(sampleindices)
                            dset_sample = vcat([dset[sampleindex, remainingcolons...] for sampleindex in sampleindices]...)
                        end
                    end
                    
                    # Ensure that we have at least an empty initial array
                    if isnothing(dset_sample)
                        dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
                    end

                    # Extend or chop sample as needed
                    samplelength = getsamplenrows(datalength)
                    # TODO: Warn about the sample size being too large
                    if size(sample, 1) < samplelength
                        dset_sample = vcat(
                            dset_sample,
                            dset[1:(samplelength-size(sample, 1)), remainingcolons...],
                        )
                    else
                        dset = dset[1:samplelength, remainingcolons...]
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
        ("Remote", Dict("path" => remotepath, "subpath" => datasetpath))

    # Get the remote sample
    if isnothing(remote_sample)
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        elseif totalnrows <= MAX_EXACT_SAMPLE_LENGTH
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

function get_remote_table_location(remotepath, remote_sample=nothing; shuffled=false, similar_files=false)::Location
    p = download_remote_path(remotepath)

    # TODO: Support more cases beyond just single files and all files in
    # given directory (e.g., wildcards)

    nbytes = 0
    totalnrows = 0

    # Read through dataset by row
    p_isdir = isdir(p)
    p_isfile = !p_isdir && isfile(p) # <-- avoid expensive unnecessary S3 API calls
    # TODO: Support more than just reading/writing single HDF5 files and
    # reading/writing directories containing CSV/Parquet/Arrow files
    files = []
    # TODO: Check for presence of cached file here and job configured to use
    # cache before proceeding
    exactsample = DataFrame()
    randomsample = DataFrame()
    files_to_read_from = if p_isdir
        Random.shuffle(readdir(p))
    elseif p_isfile
        [p]
    else
        []
    end
    for (fileidx, filep) in enumerate(files_to_read_from)
        filenrows = 0
        # TODO: Ensure usage of Base.summarysize is reasonable
        # if endswith(filep, ".csv")
        #     for chunk in CSV.Chunks(filep)
        #         chunkdf = chunk |> DataFrames.DataFrame
        #         # chunknrows = chunk.rows
        #         chunknrows = nrow(chunkdf)
        #         filenrows += chunkrows
        #         totalnrows += chunkrows

        #         # Append to randomsample
        #         # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
        #         chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
        #         # if any(chunksampleindices)
        #         if !isempty(chunksampleindices)
        #             append!(randomsample, @view chunkdf[chunksampleindices, :])
        #         end

        #         # Append to exactsample
        #         samplenrows = getsamplenrows(totalnrows)
        #         if nrow(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
        #         end

        #         nbytes += Base.summarysize(chunkdf)
        #     end
        # elseif endswith(filep, ".parquet")
        #     # TODO: Ensure estimating size using Parquet metadata is reasonable

        #     tbl = read_parquet(filep)
        #     pqf = tbl.parfile
        #     # NOTE: We assume that Tables.partitions will return a partition
        #     # for each row group
        #     for (i, chunk) in enumerate(Tables.partitions(read_parquet(filep)))
        #         chunkdf = chunk |> DataFrames.DataFrame
        #         # chunknrows = pqf.meta.row_groups[i].num_rows
        #         chunkrows = nrow(chunkdf)
        #         filenrows += chunkrows
        #         totalnrows += chunkrows

        #         # Append to randomsample
        #         # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
        #         chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
        #         # if any(chunksampleindices)
        #         if !isempty(chunksampleindices)
        #             append!(randomsample, @view chunkdf[chunksampleindices, :])
        #         end

        #         # Append to exactsample
        #         samplenrows = getsamplenrows(totalnrows)
        #         if nrow(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
        #         end

        #         # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
        #         # nbytes = nothing
        #         nbytes += Base.summarysize(chunkdf)
        #     end
        # elseif endswith(filep, ".arrow")
        #     for (i, chunk) in enumerate(Arrow.Stream(filep))
        #         chunkdf = chunk |> DataFrames.DataFrame
        #         chunknrows = nrow(chunkdf)
        #         filenrows += chunkrows
        #         totalnrows += chunkrows

        #         # Append to randomsample
        #         # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
        #         chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
        #         # if any(chunksampleindices)
        #         if !isempty(chunksampleindices)
        #             append!(randomsample, @view chunkdf[chunksampleindices, :])
        #         end

        #         # Append to exactsample
        #         samplenrows = getsamplenrows(totalnrows)
        #         if nrow(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
        #         end

        #         # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
        #         # nbytes = nothing
        #         nbytes += Base.summarysize(chunkdf)
        #     end
        # else
        #     error("Expected .csv or .parquet or .arrow for S3FS location")
        # end

        # Get chunks to sample from
        localfilepath = p_isdir ? joinpath(p, filep) : p
        with_downloaded_path_for_reading(localfilepath) do localfilepathp
            # If the data is shuffled, we don't read it it in until we know how
            # many rows there are.
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
                    totalnrows += chunknrows

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

                    # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
                    # nbytes = nothing
                    nbytes += Base.summarysize(chunkdf)

                    @show nbytes
                    @show Int64(Sys.free_memory())
                    @show Int64(Sys.total_memory())

                    # TODO: Maybe call GC.gc() here if we get an error when sampling really large datasets
                    chunkdf = nothing
                    chunk = nothing
                    if Base.summarysize(exactsample) > cld(Sys.free_memory(), 2)
                        @warn "Sample is too large; try creating a job with a greater `sample_rate` than the number of workers (default is 2)"
                    end
                    if Base.summarysize(exactsample) + nbytes > Sys.free_memory()
                        GC.gc()
                    end

                    # Optimized stopping condition in the case that all the files are similar
                    num_files_scanned = fileidx - 1
                    if similar_files && nrow(exactsample) == samplenrows && num_files_scanned >= cld(length(files_to_read_from), get_job().sample_rate)
                        # If the files are similar and we have looked through
                        # a percentage of files that is in accordance with the
                        # sample rate, we can stop. We also make sure we have
                        # enough of the exact sample in case the random sample
                        # comes short because of the random selection.
                        break
                    end
                end
            else
                filenrows = if endswith(localfilepathp, ".csv")
                    sum((1 for row in CSV.Rows(localfilepathp)))
                elseif endswith(localfilepathp, ".parquet")
                    nrows(Parquet.File(localfilepathp))
                elseif endswith(localfilepathp, ".arrow")
                    rowcount(Arrow.Table(localfilepathp))
                else
                    error("Expected .csv or .parquet or .arrow")
                end
                totalnrows += filenrows

                # TODO: Maybe also compute nbytes or perhaps it's okay to just
                # use the sample to estimate the total memory usage
            end

            # Add to list of file metadata
            push!(
                files,
                Dict(
                    "path" => p_isdir ? joinpath(remotepath, filep) : remotepath,
                    "nrows" => filenrows,
                ),
            )
        end

        # Optimized stopping condition in the case that all the files are similar
        num_files_scanned = fileidx
        if similar_files && nrow(exactsample) == samplenrows && num_files_scanned >= cld(length(files_to_read_from), get_job().sample_rate)
            break
        end
    end

    # Optimized sample collection if the data is shuffled
    if isnothing(remote_sample) && shuffled
        # We should now know exactly how many rows there are and how many
        # to sample.
        samplenrows = getsamplenrows(totalnrows)

        # So we can iterate through the files (reverse in case some caching helps
        # us)
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
                    if nrow(exactsample) < samplenrows
                        append!(exactsample, first(chunkdf, samplenrows - nrow(exactsample)))
                    end

                    # Stop as soon as we get our sample
                    if nrow(exactsample) == samplenrows
                        break
                    end
                end
            end

            # Stop as soon as we get our sample
            if nrow(exactsample) == samplenrows
                break
            end
        end
    end

    # Adjust sample to have samplenrows
    if isnothing(remote_sample)
        samplenrows = getsamplenrows(totalnrows) # Either a subset of rows or the whole thing
        if is_debug_on()
            @show samplenrows
        end
        # If we already have enough rows in the exact sample...
        if totalnrows <= MAX_EXACT_SAMPLE_LENGTH
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

    # TODO: Build up sample and return

    # Load metadata for reading
    loc_for_reading, metadata_for_reading = if !isempty(files) || p_isdir # empty directory can still be read from
        ("Remote", Dict("path" => remotepath, "files" => files, "nrows" => totalnrows))
    else
        ("None", Dict{String,Any}())
    end

    # Load metadata for writing
    # NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
    # or CSV dataset is desired to be created
    loc_for_writing, metadata_for_writing = ("Remote", Dict("path" => remotepath))

    # TODO: Cache sample on disk

    # Get remote sample
    if isnothing(remote_sample)
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        elseif totalnrows <= MAX_EXACT_SAMPLE_LENGTH
            ExactSample(randomsample, total_memory_usage = nbytes)
        else
            Sample(randomsample, total_memory_usage = nbytes)
        end
    end

    # Construct location with metadata
    Location(
        loc_for_reading,
        loc_for_writing,
        metadata_for_reading,
        metadata_for_writing,
        remote_sample,
    )
end