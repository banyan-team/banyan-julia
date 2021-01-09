using MPI

############################
# TEMPORARY TEST FUNCTIONS #
############################

function split(pt_name::String, type::String, args...)
    SPLIT_IMPL[pt_name][type](args)
end

function merge(pt_name::String, type::String, args...)
    MERGE_IMPL[pt_name][type](args)
end

# src, part, parameters::Vector{Any}, idx::Int64, nbatches::Int64; comm::MPI.Comm, lt_params
# part is a reference

##################
# BASE FUNCTIONS #
##################

default_batches_func = function(
    src, part, parameters, idx, nbatches
)
end

default_workers_func = function(
    src, part, parameters, comm
)
end

default_lt_func = function(
    src, part, parameters, idx, nbatches, comm, lt_params
)
end



###################
# SPLIT_IMPL FUNCTIONS #
###################

# PT Name --> LT_Name/Workers/Batches --> Implementation
SPLIT_IMPL = Dict{String, Dict}()

SPLIT_IMPL["Value"] = Dict{String, Any}()

SPLIT_IMPL["Value"]["Batches"] = function(
    src, part, splitting_parameters, idx, nbatches
)
    part = src
end

SPLIT_IMPL["Value"]["Workers"] = function(
    src, part, splitting_parameters, comm
)
    part = src
end

SPLIT_IMPL["Value"]["None"] = function(
    src, part, splitting_parameters, idx, nbatches, comm, lt_params
)
    part = splitting_parameters[1]
end

SPLIT_IMPL["Div"] = Dict{String, Any}()

SPLIT_IMPL["Div"]["Batches"] = function(
    src, part, splitting_parameters, idx, nbatches
)
    part = fld(src, nbatches)
end

SPLIT_IMPL["Div"]["Workers"] = function(
    src, part, splitting_parameters, comm
)
    part = fld(src, nbatches)
end

SPLIT_IMPL["Div"]["None"] = function(
    src, part, splitting_parameters, idx, nbatches, comm, lt_params
)
    part = flt(splitting_parameters[1], nbatches)
end

SPLIT_IMPL["Replicate"] = Dict{String, Any}()

SPLIT_IMPL["Replicate"]["Batches"] = function()

end

SPLIT_IMPL["Replicate"]["Workers"] = function()
end

SPLIT_IMPL["Replicate"]["Client"] = function(
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

SPLIT_IMPL["Block"] = Dict{String, Any}()

SPLIT_IMPL["Block"]["Batches"] = function(
    src, part, splitting_parameters, idx, nbatches
)

    if src[] == nothing
        part[] = nothing
    else
        dim = splitting_parameters[1]
        partition_length = cld(size(src[], dim), nbatches)

        first_idx = min(1 + idx * partition_length, size(src[], dim) + 1)
        last_idx = min((idx + 1) * partition_length, size(src[], dim))
        
        # part is a view into src
        part[] = selectdim(src[], dim, first_idx:last_idx)
    end
end

SPLIT_IMPL["Block"]["Workers"] = function(
    src, part, splitting_parameters, idx, nbatches, comm
)

    if src[] == nothing
        part[] = nothing
    else
        dim = pt.splitting_parameters[1]
        partition_length = cld(size(src[], dim), nbatches)

        # TODO: Scatter or Scatter!
        # TODO: Or should this be idx == 0? replace 0 below with idx then
        # if MPI.Comm_rank(comm) == 0
        #     Scatter!(src, nothing, partition_length, 0, comm)
        # else
        #     Scatter!(nothing, buf, partition_length, 0, comm)        
        # end
        part[] = Scatter(src[], partition_length, 0, comm)
    end

end

SPLIT_IMPL["Block"]["None"] = default_lt_func

SPLIT_IMPL["Stencil"] = Dict{String, Any}()

SPLIT_IMPL["Stencil"]["Batches"] = function (
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

SPLIT_IMPL["Stencil"]["Workers"] = function (
    src, part, splitting_parameters, idx, nbatches, comm
)
    # TODO: Implement this


    


end

SPLIT_IMPL["Stencil"]["None"] = default_lt_func


###################
# MERGE_IMPL FUNCTIONS #
###################

# PT Name --> LT_Name/Workers/Batches --> Implementation
MERGE_IMPL = Dict{String, Dict}()

MERGE_IMPL["Value"] = Dict{String, Any}()

MERGE_IMPL["Value"]["Workers"] = default_workers_func

MERGE_IMPL["Value"]["Batches"] = default_batches_func

MERGE_IMPL["Value"]["None"] = default_lt_func

MERGE_IMPL["Div"] = Dict{String, Any}()

MERGE_IMPL["Div"]["Workers"] = default_workers_func

MERGE_IMPL["Div"]["Batches"] = default_batches_func

MERGE_IMPL["Div"]["None"] = default_lt_func

MERGE_IMPL["Replicate"] = Dict{String, Any}()

MERGE_IMPL["Replicate"]["Workers"] = function()
end

MERGE_IMPL["Replicate"]["Workers"] = function()
end

MERGE_IMPL["Replicate"]["Client"] = function (
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

MERGE_IMPL["Block"] = Dict{String, Any}()

MERGE_IMPL["Block"]["Batches"] = default_batches_func

MERGE_IMPL["Block"]["Workers"] = function(
    src, part, merge_params, comm
)
    if src[] == nothing
        println("src is nothing in merge block workers")
        # TODO: Implement this case
    else
        # TODO: Allgather or Allgather!
        src[] = Allgather(part[], comm)
    end
end

MERGE_IMPL["Block"]["None"] = default_lt_func

MERGE_IMPL["Stencil"] = Dict{String, Any}()

MERGE_IMPL["Stencil"]["Batches"] = default_batches_func

MERGE_IMPL["Stencil"]["Workers"] = function (
    src, part, merge_params, comm
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

MERGE_IMPL["Stencil"]["None"] = default_lt_func


##################
# CAST FUNCTIONS #
##################

# MERGE_IMPL Name --> Split Name --> Implementation
CAST = Dict{String, Dict}()
