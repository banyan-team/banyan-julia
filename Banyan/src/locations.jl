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
    total_memory_usage::Int64

    function Location(name::String, parameters::Dict, total_memory_usage::Int64)
        new(name, name, parameters, parameters, total_memory_usage)
    end

    function Location(name::String, parameters::Dict, total_memory_usage::Int64, purpose::Symbol)
        if purpose == :src
            new(name, nothing, parameters, Dict(), total_memory_usage)
        elseif purpose == :dst
            new(nothing, name, Dict(), parameters, total_memory_usage)
        else
            error("Expected location to have purpose of either source or destination")
        end
    end

    function Location(
        src_name::Union{String, Nothing},
        src_parameters::Dict,
        dst_name::Union{String, Nothing},
        dst_parameters::Dict,
        total_memory_usage::Int64,
    )
        new(
            src_name,
            dst_name,
            src_parameters,
            dst_parameters,
            total_memory_usage,
        )
    end
end

function Base.getproperty(l::Location, name::Symbol)
    if hasfield(Location, name)
        return getfield(l, name)
    end

    n = string(name)
    if !isnothing(l.src_name) && haskey(l.src_parameters, n)
        l.src_parameters[n]
    elseif !isnothing(l.dst_name) && haskey(l.dst_parameters, n)
        l.dst_parameters[n]
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
        "total_memory_usage" => lt.total_memory_usage,
    )
end

####################
# Simple locations #
####################

None() = Location("None", Dict(), 0)
Value(val) = Location("Value", Dict("value" => to_jl_value(val)), 0)

to_jl_value(jl) =
    if jl isa Dict
        Dict(k => to_jl_value(v) for (k, v) in jl)
    elseif jl isa Vector

        [to_jl_value(e) for e in jl]
    elseif jl isa String ||
           jl isa Nothing ||
           jl isa Bool ||
           jl isa Int32 ||
           jl isa Float32
        # For cases where exact type is not important
        jl
    elseif jl isa Number
        # For cases where it can be parsed from a string
        Dict("banyan_type" => string(typeof(jl)), "contents" => string(jl))
    else
        # For DataType
        Dict("banyan_type" => "value", "contents" => string(jl))
    end

# NOTE: Currently, we only support s3:// or http(s):// and only either a
# single file or a directory containing files that comprise the dataset.

###############################
# Metadata for location paths #
###############################

struct S3Metadata
    bucket::String
    key::String
end

function s3_path_to_metadata(path::S3Path)::Vector{S3Metadata}
    if !s3_exists(path.bucket, path.key)
        return []
    end

    metadata = []
    if isdir(path)
        for filename in sort(readdir(path))
            joinedpath = joinpath(path, filename)
            push!(metadata, S3Metadata(joinedpath.bucket, joinedpath.key))
        end
    else
        push!(metadata, S3Metadata(path.bucket, path.key))
    end
    metadata
end

####################################
# Metadata for location data types #
####################################

struct CSVMetadata
    nrows::Int64
    ncolumns::Int64
    nbytes::Int64
end

function get_csv_metadata(io::IOBuffer)::CSVMetadata
    rows = CSV.Rows(io, reusebuffer=true)
    ncolumns = length(rows.columns)
    nrows = 1
    nbytes = 0
    for row in rows
        for i in 1:ncolumns
            nbytes += sizeof(CSV.detect(row, i))
        end
        nrows += 1
    end
    CSVMetadata(nrows, ncolumns, nbytes)
end

get_csv_metadata(m::S3Metadata)::CSVMetadata =
    get_csv_metadata(IOBuffer(s3_get(m.bucket, m.key)))

get_csv_metadata_from_url(url::String)::CSVMetadata =
    try
        get_csv_metadata(HTTP.get(url))
    catch e
        error("Failed to download and parse CSV file at $url")
    end

####################
# Remote locations #
####################

function CSVPath(pathname::String)
    # This function constructs a Location for reading from and/or writing to
    # with CSV.

    if startswith(pathname, "s3://")
        # Metadata for reading
        files_metadata = []
        nbytes = 0
        nrows = 0
        p = S3Path(pathname)
        for s3_metadata in s3_path_to_metadata(p)
            csv_metadata = get_csv_metadata(s3_metadata)
            push!(
                files_metadata,
                Dict(
                    "s3_bucket" => s3_metadata.bucket,
                    "s3_key" => s3_metadata.key,
                    "nrows" => csv_metadata.nrows
                )
            )
            nbytes += csv_metadata.nbytes
            nrows += csv_metadata.nrows
        end
        loc_for_reading, metadata_for_reading =
            if !isempty(files_metadata)
                ("CSV", Dict("files" => files_metadata, "nrows" => nrows))
            else
                (nothing, Dict())
            end

        # Metadata for writing
        loc_for_writing, metadata_for_writing = if isdir(p)
            (
                "CSV",
                Dict(
                    "s3_bucket" => p.bucket,
                    "s3_key" => p.key,
                    "s3_bucket_exists" => s3_exists(p.bucket, ""),
                    "nrows" => 0,
                ),
            )
        else
            (nothing, Dict())
        end

        Location(loc_for_reading, metadata_for_reading, loc_for_writing, metadata_for_writing, nbytes)
    elseif startswith(pathname, "http://") || startswith(pathname, "https://")
        metadata = get_csv_metadata_from_url(pathname)
        Location(
            "CSV",
            Dict("url" => pathname, "nrows" => metadata.nrows),
            metadata.nbytes,
            :src
        )
    else
        error("Expected either s3:// or http(s)://")
    end
end