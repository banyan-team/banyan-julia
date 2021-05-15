#################
# Location type #
#################

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::Union{String, Nothing}
    dst_name::Union{String, Nothing}
    src_parameters::Dict
    dst_parameters::Dict
    sample::Sample

    function Location(
        src_name::Union{String, Nothing},
        src_parameters::Dict,
        dst_name::Union{String, Nothing},
        dst_parameters::Dict,
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

Location(purpose::Symbol, name::String, parameters::Dict, sample::Sample = Sample()) =
    if purpose == :src
        Location(name, parameters, nothing, Dict(), sample)
    elseif purpose == :dst
        Location(nothing, Dict(), name, parameters, sample)
    else
        error("Expected location to have purpose of either source or destination")
    end

# TODO: Determine where we want syntax getproperty/setproperty! for convenience

# function Base.getproperty(l::Location, n::Symbol)
#     if hasfield(Location, n)
#         return getfield(l, n)
#     end

#     if !isnothing(l.src_name) && haskey(l.src_parameters, n)
#         l.src_parameters[n]
#     elseif !isnothing(l.dst_name) && haskey(l.dst_parameters, n)
#         l.dst_parameters[n]
#     else
#         error("$name not found in location parameters")
#     end
# end

# function Base.setproperty!(l::Location, n::Symbol, value::Any)
#     if hasfield(Location, n)
#         return setfield!(l, n, value)
#     end

#     # NOTE: This only allows setting values of parameters already in the
#     # location
#     if !isnothing(l.src_name) && haskey(l.src_parameters, n)
#         l.src_parameters[n] = value
#     end
#     if !isnothing(l.dst_name) && haskey(l.dst_parameters, n)
#         l.dst_parameters[n] = value
#     end
# end

function to_jl(lt::Location)
    return Dict(
        "src_name" => lt.src_name,
        "dst_name" => lt.dst_name,
        "src_parameters" => lt.src_parameters,
        "dst_parameters" => lt.dst_parameters,
        # NOTE: sample.properties[:rate] is always set in the Sample
        # constructor to the configured sample rate (default 1/nworkers) for
        # this job
        "total_memory_usage" => sample_memory_usage(lt.sample.value) * lt.sample.properties[:rate]
    )
end

####################
# Simple locations #
####################

Value(val) = Location(
    :src,
    "Value",
    Dict("value" => to_jl_value(val)),
    Sample(val)
)
Client(val) = Location(:src, "Client", Dict(), Sample(val))
Client() = Location(:dst, "Client", Dict())
# TODO: Un-comment only if Size is needed
# Size(size) = Value(size)
None() = Location("None", Dict(), Sample(nothing))

######################################################
# Helper functions for serialization/deserialization #
######################################################

to_jl_value(jl) =
    Dict(
        "is_banyan_value" => true,
        "contents" => to_jl_value_contents(jl)
    )

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

####################
# Remote locations #
####################

function Remote(p)
    # TODO: Support more cases beyond just single files and all files in
    # given directory (e.g., wildcards)

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

        nbytes = 0
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

            nbytes = sizeof(dset)
            datasize = size(dset)
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

        # Load metadata for writing
        loc_for_writing, metadata_for_writing =
            ("Remote", Dict("path" => filename, "subpath" => datasetpath))

        # Construct location with metadata
        return Location(
            loc_for_reading,
            metadata_for_reading,
            loc_for_writing,
            metadata_for_writing,
            nbytes,
            Sample() # TODO: Get sample of the array
        )
    end

    # Handle multi-file tabular datasets

    # Read through dataset by row
    p_isdir = isdir(p)
    nrows = 0
    nbytes = 0
    files = []
    # TODO: Check for presence of cached file here and job configured to use
    # cache before proceeding
    sample = DataFrame()
    for filep in if p_isdir sort(readdir(p)) else [p] end
        filenrows = 0
        # TODO: Ensure usage of Base.summarysize is reasonable
        if endswith(filep, ".csv")
            rows = CSV.Rows(filep, reusebuffer=true)
            ncolumns = length(rows.columns)
            for row in rows()
                for i in 1:ncolumns
                    parsedrow = CSV.detect(row, i)
                    nbytes += Base.summarysize(parsedrow)
                    # TODO: Fix performance issue here
                    if rand() < get_job().sample_rate
                        push!(sample, NamedTuple(parsedrow))
                    end
                end
                filenrows += 1
            end
        elseif endswith(filep, ".parquet")
            # TODO: Ensure estimating size using Parquet metadata is reasonable
            pqf = Parquet.File(filep)
            nbytes += sum([
                rg.total_byte_size
                for rg in pqf.meta.row_groups
            ])
            filenrows += nrows(pqf)
            # TODO: Use register_sample_computation to delay loading sample
        elseif endswith(filep, ".arrow")
            for tbl in Arrow.Stream(filep)
                nbytes += Base.summarysize(tbl)
                filenrows += size(DataFrame(tbl), 1)
            end
            # TODO: Use register_sample_computation to delay loading sample
        else
            error("Expected .csv or .parquet or .arrow for S3FS location")
        end
        push!(files, Dict("path" => filep, "nrows" => filenrows))
        nrows += filenrows
    end

    # TODO: Build up sample and return

    # Load metadata for reading
    loc_for_reading, metadata_for_reading =
        if !isempty(files_metadata)
            ("Remote", Dict("files" => files, "nrows" => nrows))
        else
            (nothing, Dict())
        end

    # Load metadata for writing
    loc_for_writing, metadata_for_writing = if p_isdir
        ("Remote", Dict("path" => p))
    else
        (nothing, Dict())
    end

    # Construct location with metadata
    Location(
        loc_for_reading,
        metadata_for_reading,
        loc_for_writing,
        metadata_for_writing,
        nbytes
    )
end

S3FS(path::String, mount::String) =
    Remote(joinpath(mount, S3Path(path).key))

URL(path::String) = Remote(download(path))

Remote(path::String; mount=nothing) =
    isnothing(mount) ? URL(path) : S3FS(path, mount)