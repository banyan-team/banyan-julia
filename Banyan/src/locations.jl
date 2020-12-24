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