#################
# Location type #
#################

mutable struct Location
    src_name::String
    dst_name::String
    src_parameters::Dict
    dst_parameters::Dict
    total_memory_usage::Int64

    function Location(name::String, parameters::Dict, total_memory_usage::Int64)
        new(name, name, parameters, parameters, total_memory_usage)
    end

    function Location(
        src_name::String,
        dst_name::String,
        src_parameters::Dict,
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

function s3_path_to_metadata(pathname::String)::Vector{S3Metadata}
    metadata = []
    path = Path(pathname)
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

get_csv_metadata(m::S3Metadata)::CSVMetadata = get_csv_metadata(s3_get(m.bucket, m.key))

get_csv_metadata_from_url(url::String)::CSVMetadata = get_csv_metadata(HTTP.get(url))

####################
# Remote locations #
####################

function CSV(pathname::String)
    if startswith(path, "s3://")
        files_metadata = []
        nbytes = 0
        nrows = 0
        for s3_metadata in s3_path_to_metadata(pathname)
            csv_metadata = get_csv_metadata(s3_metadata)
            push!(files_metadata, Dict(
                "s3_bucket" => s3_metadata.bucket,
                "s3_key" => s3_metadata.key,
                "nrows" => csv_metadata.nrows
            ))
            nbytes += csv_metadata.nbytes
            nrows += csv_metadata.nrows
        end
        Location("CSV", Dict("files" => files_metadata, "nrows" => nrows), nbytes)
    elseif startswith(path, "http://") || startswith(path, "https://")
        metadata = get_csv_metadata_from_url(pathname)
        Location(
            "CSV",
            Dict("url" => pathname, "nrows" => metadata.nrows),
            metadata.nbytes,
        )
    else
        error("Expected either s3:// or http(s)://")
    end
end