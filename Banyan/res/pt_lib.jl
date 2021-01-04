

############################
# TEMPORARY TEST FUNCTIONS #
############################

function split(pt_name::String, type::String, args...)
    SPLIT[pt_name][type](args)
end

function merge(pt_name::String, type::String, args...)
    MERGE[pt_name][type](args)
end

# src, part, parameters::Vector{Any}, idx::Int64, nbatches::Int64; comm::MPI.Comm, lt_params
# part is a reference

##################
# BASE FUNCTIONS #
##################

default_batches_func = function(
    src, part, parameters::Vector{Any}, idx, nbatches
)
end

default_workers_func = function(
    src, part, parameters::Vector{Any}, comm::MPI_Comm
)
end

default_lt_func = function(
    src, part, parameters::Vector{Any}, idx, nbatches, comm, lt_params
)
end



###################
# SPLIT FUNCTIONS #
###################

# PT Name --> LT_Name/Workers/Batches --> Implementation
SPLIT = Dict{String, Dict}()

SPLIT["Value"]["Batches"] = function(
    src, part, splitting_parameters, idx, nbatches
)
    part = src
end

SPLIT["Value"]["Workers"] = function(
    src, part, splitting_parameters, comm::MPI_Comm
)
    part = src
end

SPLIT["Value"]["None"] = function(
    src, part, splitting_parameters, idx, nbatches, comm, lt_params
)
    part = splitting_parameters[1]
end

SPLIT["Div"]["Batches"] = function(
    src, part, splitting_parameters, idx, nbatches
)
    part = fld(src, nbatches)
end

SPLIT["Div"]["Workers"] = function(
    src, part, splitting_parameters, comm::MPI_Comm
)
    part = fld(src, nbatches)
end

SPLIT["Div"]["None"] = function(
    src, part, splitting_parameters, idx, nbatches, comm, lt_params
)
    part = flt(splitting_parameters[1], nbatches)
end

SPLIT["Replicate"]["Batches"] = function()

end

SPLIT["Replicate"]["Workers"] = function()
end

SPLIT["Replicate"]["Client"] = function(
    src, part, splitting_parameters, idx, nbatches, comm, lt_params
)

    global comm  # TODO: not necessarily

    # Main worker sends scatter request on gather queue
    #   and waits for response on scatter queue
    value_id = pt.splitting_parameters[1]
    v = nothing
    if MPI.Comm_rank(comm) == 0
        send_scatter_request(value_id)

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
    src, part, splitting_parameters, idx, nbatches
)
    dim = splitting_parameters[1]
    partition_length = cld(size(src, dim), nbatches)

    first_idx = min(1 + idx * partition_length, size(src, dim) + 1)
    last_idx = min((idx + 1) * partition_length, size(src, dim))
    
    # part is a view into src
    part = selectdim(src, dim, first_idx:last_idx)
end

# TODO: Make sure this is correct
SPLIT["Block"]["Workers"] = function(
    src, part, splitting_parameters, idx, nbatches, comm::MPI_Comm
)
    dim = pt.splitting_parameters[1]
    partition_length = cld(size(src, dim), nbatches)

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

SPLIT["Stencil"]["Batches"] = function (
    src, part, splitting_parameters, idx, nbatches
)
    dim = splitting_parameters[1]
    size = splitting_parameters[2]
    stride = splitting_parameters[3]
    @assert length(size) = len(stride)

    num_blocks = cld(cld(size(src, dim) - size, stride), nbatches)
    first_idx = 1 + idx * stride * num_blocks
    last_idx = min(1 + idx * stride * num_blocks + size, size(src, dim))

    part = selectdim(src, dim, first_idx:last_idx)
end

SPLIT["Stencil"]["Workers"] = function (
    src, part, splitting_parameters, idx, nbatches, comm::MPI_Comm
)
    # TODO: Implement this


    


end

SPLIT["Stencil"]["None"] = default_lt_func


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
    src, part, merge_params, idx, nbatches, comm, lt_params
)

    # TODO: Get comm
    global comm

    value_id = merge_params[1]
    if MPI.Comm_rank(comm) == 0
        buf = IOBuffer()
        serialize(buf, part)
        value = take!(buf)
        send_gather(value_id, value)
    end
end

MERGE["Block"]["Batches"] = default_batches_func

MERGE["Block"]["Workers"] = function(
    src, part, merge_params, comm::MPI_Comm
)

    if src == nothing
        # TODO: Implement this case
    else
        # TODO: Allgather or Allgather!
        src = Allgather(part, comm)
    end
end

MERGE["Block"]["None"] = default_lt_func

MERGE["Stencil"]["Batches"] = default_batches_func

MERGE["Stencil"]["Workers"] = function (
    src, part, merge_params, comm::MPI_Comm
)
    # TODO: Implement this

    if src == nothing
        # TODO: Implement this case
    else
    #     dim = splitting_parameters[1]
    #     size = splitting_parameters[2]
    #     stride = splitting_parameters[3]

    #     overlap = [min(, 0) for s in size]

    # num_blocks = cld(cld(size(src, dim) - size, stride), nbatches)
    # first_idx = 1 + idx * stride * num_blocks
    # last_idx = min(1 + idx * stride * num_blocks + size, size(src, dim))
    end
end

MERGE["Stencil"]["None"] = default_lt_func


##################
# CAST FUNCTIONS #
##################

# MERGE Name --> Split Name --> Implementation
CAST = Dict{String, Dict}()
