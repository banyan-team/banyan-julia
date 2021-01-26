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