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
Value(val) = Location("Value", "Value", Dict("value" => to_jl_value(val)), Dict("value" => to_jl_value(val)), 0)

to_jl_value(jl) =
    if jl isa Dict
        Dict(k => to_jl_value(v) for (k, v) in jl)
    elseif jl isa Vector
        [to_jl_value(e) for e in jl]
    elseif jl isa String || jl isa Nothing || jl isa Bool || jl isa Int32 || jl isa Float32
        # For cases where exact type is not important
        jl
    elseif jl isa Number
        # For cases where it can be parsed from a string
        Dict(
            "banyan_type" => string(typeof(jl)),
            "contents" => string(jl)
        )
    else
        # For DataType
        Dict(
            "banyan_type" => "value",
            "contents" => string(jl)
        )
    end