struct LocationType
    name::String
    data_size::Int64
    parameters::Vector{Any}
end

function lt_to_jl(lt::LocationType)
    return Dict(
    	"name" => lt.location_name,
    	"data_size" => lt.data_size,
    	"parameters" => lt.parameters
    )
end

struct LocationTypes
    location_types::Dict{ValueId, LocationType}
end

function to_jl(lts:LocationTypes)
    return Dict(
        "location_types" => {value_id => lt_to_jl(lt) for (value_id, lt) in lts.location_types}
    )
end