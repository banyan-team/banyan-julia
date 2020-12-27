

# src, part, pt::PartitionType, idx::Int64, npartitions::Int64; comm::MPI.Comm, lt::LocationType
# part is a reference

##################
# BASE FUNCTIONS #
##################

default_batches_func = function(
    src, part, pt::PartitionType, idx, npartitions
)
end

default_workers_func = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)
end

default_lt_func = function(
    src, part, pt::PartitionType, idx, npartitions, lt::LocationType
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

SPLIT["Replicate"]["Batches"] = function()

end

SPLIT["Replicate"]["Workers"] = function()
end

SPLIT["Replicate"]["Client"] = function(
    src, part, pt::PartitionType, idx, npartitions, lt::LocationType
)

    global comm  # TODO: not necessarily

    # Main worker sends scatter request on gather queue
    #   and waits for response on scatter queue
    value_id = pt.splitting_parameters[1]
    v = nothing
    if MPI.Comm_rank(comm) == 0
        sqs_send_message(
            get_gather_queue()
            JSON.json(Dict(
                "kind" => "SCATTER_REQUEST",
                "value_id" => value_id
            )),
            (:MessageGroupId, "1"),
            (:MessageDeduplicationId, get_message_id())
        )

        m = nothing
        while (isnothing(m))
            m = sqs_receive_message(get_scatter_queue())
        end
        v = JSON.parse(m[:message])["value"]
        v = deserialize(IOBuffer(convert(Array{Uint8}, v,)))
        sqs_delete_message(get_scatter_queue(), m)
    end

    part = MPI.bcast(v, 0, comm)

end


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

MERGE["Replicate"]["Workers"] = function()
end

MERGE["Replicate"]["Workers"] = function()
end

MERGE["Replicate"]["Client"] = function (
    src, part, pt::PartitionType, idx, npartitions, lt::LocationType
)
    value_id = pt.merging_parameters[1]
    if MPI.Comm_rank(comm) == 0
        buf = IOBuffer()
        serialize(buf, part)
        value = take!(buf)
        sqs_send_message(
            get_gather_queue(),
            JSON.json(Dict(
                "kind" => "GATHER",
                "value_id" => value_id,
                "value" => value
            )),
            (:MessageGroupId, "1"),
            (:MessageDeduplicationId, get_message_id())
        )
    end
end

MERGE["Block"]["Batches"] = default_batches_func

MERGE["Block"]["Workers"] = function(
    src, part, pt::PartitionType, idx, npartitions, comm::MPI_Comm
)

    if src == nothing
        # TODO: Implement this case
    else
        # TODO: Allgather or Allgather!
        src = Allgather(part, comm)
    end
end

MERGE["Block"]["None"] = default_lt_func


##################
# CAST FUNCTIONS #
##################

# MERGE Name --> Split Name --> Implementation
CAST = Dict{String, Dict}()
