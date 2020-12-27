

# src, part, pt::PartitionType, idx::Int64, npartitions::Int64; comm::MPI.Comm, lt::LocationType
# part is a reference

##################
# BASE FUNCTIONS #
##################

default_lt_func = function(
    src, part, pt::PartitionType, idx, npartitions, lt::LocationType
)
end

default_batches_func = function(
    src, part, pt::PartitionType, idx, npartitions
)
end

default_workers_func = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)
end



###################
# SPLIT FUNCTIONS #
###################

# PT Name --> LT_Name/Workers/Batches --> Implementation
SPLIT = Dict{String, Dict}()

SPLIT["Value"]["Batches"] = function(
    src, part, pt::PartitionType, idx, npartitions
)
    part = pt.splitting_parameters[1]
end

SPLIT["Value"]["Workers"] = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)
    part = pt.splitting_parameters[1]
end

SPLIT["Value"]["None"] = default_lt_func

SPLIT["Div"]["Batches"] = function(
    src, part, pt::PartitionType, idx, npartitions
)
    part = fld(src, npartitions)
end

SPLIT["Div"]["Workers"] = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)
    part = fld(src, npartitions)
end

SPLIT["Div"]["None"] = default_lt_func

SPLIT["Block"]["Batches"] = function(
    src, part, pt::PartitionType, idx, npartitions
)
    dim = pt.splitting_parameters[1]
    partition_length = cld(size(src, dim), npartitions)

    first_idx = min(1 + idx * partition_length, size(src, dim) + 1)
    last_idx = min((idx + 1) * partition_length, size(src, dim))
    
    # part is a view into src
    part = selectdim(src, dim, first_idx:last_idx)
end

# TODO: Make sure this is correct
SPLIT["Block"]["Workers"] = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)
    dim = pt.splitting_parameters[1]
    partition_length = cld(size(src, dim), npartitions)

    # TODO: Scatter or Scatter!
    # TODO: Or should this be idx == 0? replace 0 below with idx then
    # if MPI.Comm_rank(comm) == 0
    #     Scatter!(src, nothing, partition_length, 0, comm)
    # else
    #     Scatter!(nothing, buf, partition_length, 0, comm)        
    # end
    part = Scatter(src, partition_length, 0, comm)

end

SPLIT["Block"]["None"] = default_lt_func


###################
# MERGE FUNCTIONS #
###################

# PT Name --> LT_Name/Workers/Batches --> Implementation
MERGE = Dict{String, Dict}()

MERGE["Value"]["Workers"] = default_workers_func

MERGE["Value"]["Batches"] = default_batches_func

MERGE["Value"]["None"] = default_lt_func

MERGE["Div"]["Workers"] = default_workers_func

MERGE["Div"]["Batches"] = default_batches_func

MERGE["Div"]["None"] = default_lt_func

MERGE["Block"]["Batches"] = default_batches_func

MERGE["Block"]["Workers"] = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)

    # TODO: Allgather or Allgather!
    src = Allgather(part, comm)
end

MERGE["Block"]["None"] = default_lt_func


##################
# CAST FUNCTIONS #
##################

# MERGE Name --> Split Name --> Implementation
CAST = Dict{String, Dict}()
