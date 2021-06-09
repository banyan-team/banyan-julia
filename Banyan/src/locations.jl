#################
# Location type #
#################

const LocationParameters = Dict{String, Any}

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::Union{String, Nothing}
    dst_name::Union{String, Nothing}
    src_parameters::LocationParameters
    dst_parameters::LocationParameters
    sample::Sample

    function Location(
        src_name::Union{String, Nothing},
        src_parameters::LocationParameters,
        dst_name::Union{String, Nothing},
        dst_parameters::LocationParameters,
        sample::Sample = Sample()
    )
        if isnothing(src_name) && isnothing(dst_name)
            error("Location must either be usable as a source or as a destination for data")
        end

        new(
            src_name,
            dst_name,
            src_parameters,
            dst_parameters,
            sample
        )
    end
end

Location(name::String, parameters::Dict, sample::Sample = Sample()) =
    Location(name, name, parameters, parameters, sample)

LocationSource(name::String, parameters::Dict, sample::Sample = Sample()) =
    Location(name, parameters, nothing, Dict(), sample)

LocationDestination(name::String, parameters::Dict, sample::Sample = Sample()) =
    Location(nothing, Dict(), name, parameters, sample)

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
    return Dict(
        "src_name" => lt.src_name,
        "dst_name" => lt.dst_name,
        "src_parameters" => lt.src_parameters,
        "dst_parameters" => lt.dst_parameters,
        # NOTE: sample.properties[:rate] is always set in the Sample
        # constructor to the configured sample rate (default 1/nworkers) for
        # this job
        "total_memory_usage" => sample(lt.sample, :memory_usage) * sample(lt.sample, :rate)
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
            loc.src_parameters,
            fut_location.dst_name,
            fut_location.dst_parameters,
            max(fut.location.total_memory_usage, loc.total_memory_usage),
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
            fut_location.src_name,
            fut_location.src_parameters,
            loc.dst_name,
            loc.dst_parameters,
            max(fut.location.total_memory_usage, loc.total_memory_usage),
        ),
    )
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
end

function located(futs...)
    futs = futs .|> obj->convert(Future, obj)
    maxindfuts = argmax([
        get_location(f).total_memory_usage
        for f in futs
    ])
    for fut in futs
        located(
            fut,
            get_location(futs[maxindfuts]),
        )
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
        mem(
            fut,
            maximum([
                begin
                    get_location(f).total_memory_usage
                end
                for f in futs
            ]),
        )
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

Value(val) = LocationSource(
    "Value",
    Dict("value" => to_jl_value(val)),
    ExactSample(val)
)

# TODO: Implement Size
Size(val) = LocationSource(
    "Value",
    Dict("value" => to_jl_value(val)),
    Sample(
        div(val, get_job().sample_rate, dims=1),
        sample_rate = get_job().sample_rate,
    ),
)

Client(val) = LocationSource("Client", Dict(), ExactSample(val))
Client() = LocationDestination("Client", Dict())
# TODO: Un-comment only if Size is needed
# Size(size) = Value(size)

None() = Location("None", Dict(), Sample())
# Values assigned "None" location as well as other locations may reassigned
# "Memory" or "Disk" locations by the scheduler depending on where the relevant
# data is.

######################################################
# Helper functions for serialization/deserialization #
######################################################

to_jl_value(jl) =
    Dict(
        "is_banyan_value" => true,
        "contents" => to_jl_value_contents(jl)
    )

# NOTE: This function is copied into pt_lib.jl so any changes here should
# be made there
to_jl_value_contents(jl) =
    begin
        io = IOBuffer()
        iob64_encode = Base64EncodePipe(io)
        serialize(iob64_encode, jl)
        close(iob64_encode)
        String(take!(io))
    end

# NOTE: This function is copied into pt_lib.jl so any changes here should
# be made there
from_jl_value_contents(jl_value_contents) =
    begin
        io = IOBuffer()
        iob64_decode = Base64DecodePipe(io)
        write(io, jl_value_contents)
        seekstart(io)
        deserialize(iob64_decode)
    end

# NOTE: Currently, we only support s3:// or http(s):// and only either a
# single file or a directory containing files that comprise the dataset.

# TODO: Add support for Client

# TODO: Implement Client, Remote for HDF5, Parquet, Arrow, and CSV so that they
# compute nrows ()

####################
# Remote locations #
####################

# TODO: Call a function Cailin is making to ensure that the bucket is already
# mounted at a known location and use that returned location

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

MAX_EXACT_SAMPLE_LENGTH = 1024

getsamplenrows(totalnrows) =
    if totalnrows <= MAX_EXACT_SAMPLE_LENGTH
        totalnrows
    else
        div(totalnrows, get_job().sample_rate)
    end

function Remote(p; read_from_cache=true, write_to_cache=true)
    # Read location from cache. The location will include metadata like the
    # number of rows in each file as well as a sample that can be used on the
    # client side for estimating memory usage and data skew among other things.
    locationpath = joinpath(homedir(), ".banyan", "locations", p |> hash |> string)
    location =
        if read_from_cache && isfile(locationpath)
            deserialize(locationpath)
        else
            get_remote_location(p)
        end

    # Store location in cache
    if write_to_cache
        serialize(locationpath, location)
    end

    location
end

function get_remote_location(p)
    Random.seed!(get_job_id())
    
    # TODO: Cache stuff
    if startswith(p, "s3://")
        p = get_s3fs_path(p)
    elseif startswith(p, "http://") || startswith(p, "https://")
        p = download(p)
    else
        throw(ArgumentError("Expected location that starts with either http:// or https:// or s3://"))
    end

    # TODO: Support more cases beyond just single files and all files in
    # given directory (e.g., wildcards)

    # TODO: Read cached sample if possible

    nbytes = 0
    totalnrows = 0

    # Handle single-file nd-arrays

    hdf5_ending =
        if occursin(p, ".h5")
            ".h5"
        elseif occursin(p, ".hdf5")
            ".hdf5"
        else
            ""
        end
    if length(hdf5_ending) > 0
        filename, datasetpath = split(p, hdf5_ending)
        filename *= ".h5"

        # Load metadata for reading

        # Open HDF5 file
        sample = []
        datasize = [0]
        if isfile(p)
            f = h5open(p, "r")
            dset = f[datasetpath]
            ismapping = false
            if ismmappable(dset)
                ismapping = true
                dset = readmmap(dset)
                close(f)
            end

            # Collect metadata
            nbytes += sizeof(dset)
            datasize = size(dset)

            # Collect sample
            datalength = first(datasize)
            remainingcolons = repeat([:], ndims(dset)-1)
            if datalength < MAX_EXACT_SAMPLE_LENGTH
                sampleindices = randsubseq(1:datalength, 1 / get_job().sample_rate)
                sample = dset[sampleindices, remainingcolons...]
            end

            # Extend or chop sample as needed
            samplelength = getsamplenrows(datalength)
            if size(sample, 1) < samplelength
                sample = vcat(sample, dset[1:(samplelength - size(sample, 1)), remainingcolons...])
            else
                dset = dset[1:samplelength, remainingcolons...]
            end

            # Close HDF5 file
            if !ismapping
                close(f)
            end
        end

        loc_for_reading, metadata_for_reading = if isfile(p)
            ("Remote", Dict(
                "path" => filename,
                "subpath" => datasetpath,
                "size" => datasize
            ))
        else
            (nothing, Dict())
        end

        # Load metadata for writing to HDF5 file
        loc_for_writing, metadata_for_writing =
            ("Remote", Dict("path" => filename, "subpath" => datasetpath))

        # Construct location with metadata
        return Location(
            loc_for_reading,
            metadata_for_reading,
            loc_for_writing,
            metadata_for_writing,
            if isnothing(loc_for_reading)
                Sample()
            elseif totalnrows <= MAX_EXACT_SAMPLE_LENGTH
                ExactSample(sample, total_memory_usage=nbytes)
            else
                Sample(sample, total_memory_usage=nbytes)
            end
        )
    end

    # Handle multi-file tabular datasets

    # Read through dataset by row
    p_isdir = isdir(p)
    files = []
    # TODO: Check for presence of cached file here and job configured to use
    # cache before proceeding
    exactsample = DataFrame()
    randomsample = DataFrame()
    for filep in if p_isdir sort(readdir(p)) else [p] end
        filenrows = 0
        # TODO: Ensure usage of Base.summarysize is reasonable
        # if endswith(filep, ".csv")
        #     for chunk in CSV.Chunks(filep)
        #         chunkdf = chunk |> DataFrames.DataFrame
        #         # chunknrows = chunk.rows
        #         chunknrows = nrows(chunkdf)
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
        #         if nrows(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrows(exactsample)))
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
        #         chunkrows = nrows(chunkdf)
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
        #         if nrows(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrows(exactsample)))
        #         end

        #         # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
        #         # nbytes = nothing
        #         nbytes += Base.summarysize(chunkdf)
        #     end
        # elseif endswith(filep, ".arrow")
        #     for (i, chunk) in enumerate(Arrow.Stream(filep))
        #         chunkdf = chunk |> DataFrames.DataFrame
        #         chunknrows = nrows(chunkdf)
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
        #         if nrows(exactsample) < samplenrows
        #             append!(exactsample, first(chunkdf, samplenrows - nrows(exactsample)))
        #         end

        #         # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
        #         # nbytes = nothing
        #         nbytes += Base.summarysize(chunkdf)
        #     end
        # else
        #     error("Expected .csv or .parquet or .arrow for S3FS location")
        # end

        # Get chunks to sample from
        chunks =
            if endswith(filep, ".csv")
                CSV.Chunks(filep)
            elseif endswith(filep, ".parquet")
                Tables.partitions(read_parquet(filep))
            elseif endswith(filep, ".arrow")
                Arrow.Stream(filep)
            else
                error("Expected .csv or .parquet or .arrow")
            end

        # Sample from each chunk
        for (i, chunk) in enumerate(chunks)
            chunkdf = chunk |> DataFrames.DataFrame
            chunknrows = nrows(chunkdf)
            filenrows += chunkrows
            totalnrows += chunkrows

            # Append to randomsample
            # chunksampleindices = map(rand() < 1 / get_job().sample_rate, 1:chunknrows)
            chunksampleindices = randsubseq(1:chunknrows, 1 / get_job().sample_rate)
            # if any(chunksampleindices)
            if !isempty(chunksampleindices)
                append!(randomsample, @view chunkdf[chunksampleindices, :])
            end

            # Append to exactsample
            samplenrows = getsamplenrows(totalnrows)
            if nrows(exactsample) < samplenrows
                append!(exactsample, first(chunkdf, samplenrows - nrows(exactsample)))
            end

            # nbytes += isnothing(chunkdf) ? pqf.meta.row_groups[i].total_byte_size : Base.summarysize(chunkdf)
            # nbytes = nothing
            nbytes += Base.summarysize(chunkdf)
        end

        # Add to list of file metadata
        push!(files, Dict("path" => filep, "nrows" => filenrows))
        totalnrows += filenrows
    end

    # Adjust sample to have samplenrows
    samplenrows = getsamplenrows(totalnrows)
    if nrows(randomsample) < samplenrows
        append!(randomsample, first(exactsample, samplenrows - nrows(randomsample)))
    end
    if nrows(randomsample) > samplenrows
        randomsample = first(randomsample, samplenrows)
    end

    # TODO: Build up sample and return

    # Load metadata for reading
    loc_for_reading, metadata_for_reading =
        if !isempty(files_metadata)
            ("Remote", Dict("files" => files, "nrows" => totalnrows))
        else
            (nothing, Dict())
        end

    # Load metadata for writing
    loc_for_writing, metadata_for_writing = if p_isdir
        ("Remote", Dict("path" => p))
    else
        (nothing, Dict())
    end

    # TODO: Cache sample on disk

    # Construct location with metadata
    Location(
        loc_for_reading,
        metadata_for_reading,
        loc_for_writing,
        metadata_for_writing,
        if isnothing(loc_for_reading)
            Sample()
        elseif totalnrows <= MAX_EXACT_SAMPLE_LENGTH
            ExactSample(sample, total_memory_usage=nbytes)
        else
            Sample(sample, total_memory_usage=nbytes)
        end
    )
end
