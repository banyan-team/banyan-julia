struct LocationType
    name::String
    parameters::Vector{Any}
end

function lt_to_jl(pt::LocationType)
    buf = IOBuffer()
    return Dict("name" => lt.location_name, "parameters" => lt.parameters)
end