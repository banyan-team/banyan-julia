mutable struct Location
    src_name::String
    dst_name::String
    src_parameters::Dict
    dst_parameters::Dict
    total_memory_usage::Int64
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

None() = Location("None", "None", Dict(), Dict(), 0)
Value(val) = Location(
    "Value",
    "Value",
    Dict("value" => to_jl_value(val)),
    Dict("value" => to_jl_value(val)),
    0,
)

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

# TODO: Implement locations for s3:// and http(s)://

# struct S3Path
#     bucket::String
#     key::String
# end

# function parse_datasource_names(datasource_name::String)
#     if startswith(datasource_name, "s3://")
#         datasource_path = Path(datasource_name)
#         bucket, key = datasource_path.bucket, datasource_path.key
#         dirname, filename = splitdir(key)
#         if '*' in filename
#             [S3Path(bucket, p) for p in readdir(path) if occursin(Regex(filename), p)]
#         elseif length(filename) > 0
#             [path]
#         else
#             error("Expected either a single file or a glob of files")
#         end
#     elseif startswith(datasource_name, "http://") || startswith(datasource_name, "https://")
#         return [Path(datasource_name)]
#     else
#         error("Expected s3:// or http:// or https://")
#     end
# end

# parse_datasource_names(datasource_names::Vector{String}) =
#     vcat([parse_datasource_names(dsn for dsn in datasource_names)])

# read_datasources(datasource_names) =
#     [
#         begin
#             dsn_str = string(dsn)
#             if startswith(dsn_str, "file://")
#             elseif startswith(dsn_str, "s3://")
#         end
#         for dsn in datasource_names
#     ]

# function CSV(filename::String)
#     Location("CSV", "CSV", Dict())
# end