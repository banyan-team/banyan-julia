#########
# ENUMS #
#########



##################
# PARTITION TYPE #
##################

struct PartitionType
    split_name::String
    merge_name::String
    splitting_parameters::Vector{Any}
    merging_parameters::Vector{Any}
    max_partitions::Int32
end

function pt_to_jl(pt::PartitionType)
    return Dict(
        "split_name" => pt.split_name,
        "merge_name" => pt.merge_name,
        "splitting_parameters" => pt.splitting_parameters,
        "max_partitions" => pt.max_partitions
    )
end

######################### 
# PARTITION CONSTRAINTS #
#########################

struct PartitioningConstraints

end

function constraints_to_jl(constraints::PartitioningConstraints)
	return Dict()
end


######################## 
# PARTITION ANNOTATION #
########################

struct PartitionAnnotation
    partitions::Dict{ValueId,Dict{Level,Vector{PartitionType}}}
    constraints
end

function pa_to_jl(pa::PartitionAnnotation)
    return Dict()
end