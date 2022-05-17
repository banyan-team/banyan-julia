using Base: Integer, AbstractVecOrTuple
using MPI

####################
# Helper functions #
####################

get_worker_idx(comm::MPI.Comm = MPI.COMM_WORLD)::Int64 = MPI.Comm_rank(comm) + 1
get_nworkers(comm::MPI.Comm = MPI.COMM_WORLD)::Int64 = MPI.Comm_size(comm)
is_main_worker(comm::MPI.Comm = MPI.COMM_WORLD) = get_worker_idx(comm) == 1

get_partition_idx(batch_idx::Int64, nbatches::Int64, comm::MPI.Comm)::Int64 =
    get_partition_idx(batch_idx, nbatches, get_worker_idx(comm))

get_partition_idx(batch_idx::Int64, nbatches::Int64, worker_idx::Int64)::Int64 =
    (worker_idx - 1) * nbatches + batch_idx

get_npartitions(nbatches::Int64, comm::MPI.Comm)::Int64 = nbatches * get_nworkers(comm)

split_len(src_len::Int64, idx::Int64, npartitions::Int64)::UnitRange{Int64} =
    if npartitions > 1
        # dst_len = Int64(cld(src_len, npartitions))
        dst_len = cld(src_len, npartitions)
        dst_start = min((idx - 1) * dst_len + 1, src_len + 1)
        dst_end = min(idx * dst_len, src_len)
        dst_start:dst_end
    else
        1:src_len
    end

split_len(src_len::Int64, batch_idx::Int64, nbatches::Int64, comm::MPI.Comm)::UnitRange{Int64} = split_len(
    src_len,
    get_partition_idx(batch_idx, nbatches, comm),
    get_npartitions(nbatches, comm),
)

split_on_executor(src, d::Int64, i::UnitRange{Int64}) = error("Splitting $(typeof(src)) not supported")

split_on_executor(
    src::T,
    dim::Int64,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
) where {T} = begin
    npartitions = get_npartitions(nbatches, comm)
    if npartitions > 1
        Banyan.split_on_executor(
            src,
            dim,
            split_len(
                size(src, dim),
                get_partition_idx(batch_idx, nbatches, comm),
                npartitions,
            )
        )
    else
        src
    end
end

# Helper functions along with get_worker_idx() and get_nworkers()
split_across(obj, idx=get_worker_idx(), npartitions=get_nworkers()) = obj[split_len(length(obj), idx, npartitions)]
sync_across(;comm=MPI.COMM_WORLD) = MPI.Barrier(comm)
sync_across(obj; from_worker_idx=1, comm=MPI.COMM_WORLD) = MPI.bcast(obj, from_worker_idx-1, comm)
reduce_across(func, val; to_worker_idx=1, comm=MPI.COMM_WORLD) = MPI.Reduce(val, func, to_worker_idx-1, comm)
reduce_and_sync_across(func, val; comm=MPI.COMM_WORLD) = MPI.Allreduce(val, func, comm)

function gather_across(obj::T, comm=MPI.COMM_WORLD) where {T}
    is_main = is_main_worker(comm)
    io = IOBuffer()
    serialize(io, obj)
    sendbuf = MPI.Buffer(view(io.data, 1:io.size))
    sizes = MPI.Gather(sendbuf.count, 0, comm)
    recvvbuf = is_main ? VBuffer(similar(sendbuf.data, sum(sizes)), sizes) : nothing
    MPI.Gatherv!(sendbuf, recvvbuf, 0, comm)
    if is_main
        [
            view(
                recvvbuf.data,
                (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
            ) |> IOBuffer |> deserialize
            for i in 1:get_nworkers(comm)
        ]
    else
        T[]
    end
end

merge_on_executor(obj::Any, key) = error("Merging $(typeof(obj)) not supported")
# merge_on_executor(obj::Vector{Missing}; key=nothing) = missing
# merge_on_executor(obj::Vector; key=nothing) = if isempty(obj) missing else error("Merging $(typeof(obj)) not supported") end

# # TODO: Make `merge_on_executor` and `tobuf` and `frombuf`
# # dispatch based on the `kind` so we only have to precompile Arrow if we are
# # working with dataframes.
# function merge_on_executor(kind::Symbol, vbuf::MPI.VBuffer, nchunks::Int64; key)
#     chunks = [
#         begin
#             chunk = view(vbuf.data, (vbuf.displs[i]+1):(vbuf.displs[i]+vbuf.counts[i]))
#             if kind == :df
#                 DataFrames.DataFrame(Arrow.Table(IOBuffer(chunk)))
#             elseif kind == :bits
#                 chunk
#             else
#                 deserialize(IOBuffer(chunk))
#             end
#         end for i = 1:nchunks
#     ]
#     merge_on_executor(chunks...; key = key)
# end

reduce_worker_idx_and_val(worker_idx_and_val_a, worker_idx_and_val_b) =
    begin
        if worker_idx_and_val_a[2]
            worker_idx_and_val_a
        else
            worker_idx_and_val_b
        end
    end

function find_worker_idx_where(val::Bool; comm::MPI.Comm = MPI.COMM_WORLD, allreduce::Bool = true)::Int64
    worker_idx = get_worker_idx(comm)
    worker_idx_and_val = (worker_idx, val)
    worker_idx, aggregated_val = if allreduce
        MPI.Allreduce(worker_idx_and_val, reduce_worker_idx_and_val, comm)
    else
        res = MPI.Reduce(worker_idx_and_val, reduce_worker_idx_and_val, get_nworkers(comm)-1, comm)
        if worker_idx == 1
            res
        else
            (-1, false)
        end
    end
    aggregated_val ? worker_idx : -1
end

function get_oh_partition_idx_from_divisions(
    oh::OH,
    divisions::Base.Vector{Base.Vector{Tuple{OH,OH}}},
    boundedlower::Bool,
    boundedupper::Bool,
)::Int64 where {OH}
    # The first and last partitions (used if this lacks a lower or upper bound)
    # must have actual division(s) associated with them. If there is no
    # partition that has divisions, then they will all be skipped and -1 will
    # be returned. So these indices are only used if there are nonempty
    # divisions.
    firstdivisionidx = findfirst(isnotempty, divisions)
    lastdivisionidx = findlast(isnotempty, divisions)

    # The given divisions may be returned from `get_divisions`
    for (i, div) in enumerate(divisions)
        # Now _this_ is a plausible cause. `get_divisions` can return a bunch
        # of empty arrays and in that case we should just skip that division.
        if isempty(div)
            continue
        end

        isfirstdivision = i == firstdivisionidx
        islastdivision = i == lastdivisionidx
        if ((!boundedlower && isfirstdivision) || oh >= div[1][1]) &&
           ((!boundedupper && islastdivision) || oh < div[end][2])
            return i
        end
    end

    # We return -1 since this value doesn't belong to any of the partitions
    # represented by `divisions`.
    -1
end

function get_partition_idx_from_divisions(val::T, divisions::Base.Vector{Base.Vector{Tuple{OH,OH}}}, boundedlower::Bool, boundedupper::Bool)::Int64 where {T,OH}
    get_oh_partition_idx_from_divisions(orderinghash(val), divisions, boundedlower, boundedupper)
end

isoverlapping(a::AbstractRange, b::AbstractRange) = a.start ≤ b.stop && b.start ≤ a.stop

######################################################
# Helper functions for serialization/deserialization #
######################################################

@nospecialize

to_jl_value(jl) = Dict{String,Any}("is_banyan_value" => true, "contents" => to_jl_value_contents(jl))

# NOTE: This function is shared between the client library and the PT library
function to_jl_value_contents(jl)::String
    # Handle functions defined in a module
    # TODO: Document this special case
    # if jl isa Function && !(isdefined(Base, jl) || isdefined(Core, jl) || isdefined(Main, jl))

    # Convert Julia object to string
    io = IOBuffer()
    iob64_encode = Base64EncodePipe(io)
    serialize(iob64_encode, jl)
    close(iob64_encode)
    String(take!(io))
end

# NOTE: This function is shared between the client library and the PT library
function from_jl_value_contents(jl_value_contents::String)
    # Converty string to Julia object
    io = IOBuffer()
    iob64_decode = Base64DecodePipe(io)
    write(io, jl_value_contents)
    seekstart(io)
    res = deserialize(iob64_decode)

    # Handle functions defined in a module
    res
end


##################################################
# Order-preserving hash for computing  divisions #
##################################################

# NOTE: `orderinghash` must either return a number or a vector of
# equally-sized numbers
# NOTE: This is an "order-preserving hash function" (google that for more info)
function orderinghash(x::T)::Vector{T} where {T} T[x] end # This lets us handle numbers and dates
function orderinghash(x::I)::Vector{Int64} where I<:Integer
    Int64[convert(Int64, x)]
end
function orderinghash(x::I)::Vector{Float64} where I<:Real
    Float64[convert(Float64, x)]
end
orderinghash(s::AbstractString)::Vector{UInt8} = orderinghash(convert(String, s))
function orderinghash(s::String)::Vector{UInt8}
    s_view = view(s, 1:min(32,length(s)))
    a = codeunits(s_view)
    a_view = view(a, 1:min(32,length(a)))
    b_length = 32 - min(32,length(s))
    b = fill(0x20, b_length)
    vcat(a_view,b)
end
orderinghash(A::AA) where AA<:AbstractArray = orderinghash(first(A))

const Division{V} = Tuple{V,V} where {V <: AbstractVector}

function get_divisions(divisions::Base.Vector{Division{V}}, npartitions::Int64)::Base.Vector{Base.Vector{Division{V}}} where V
    # This function accepts a list of divisions where each division is a tuple
    # of ordering hashes (values returned by `orderinghash` which are either
    # numbers or vectors of numbers). It also accepts a number of partitions to
    # produce divisions for. The result is a list of length `npartitions`
    # containing lists of divisions for each partition. A partition may contain
    # multiple divisions.

    ndivisions::Int64 = length(divisions)
    if ndivisions == 0
        # If there are no divisions (maybe this dataset or this partition of a
        # dataset is empty), we simply return empty set.
        map(_->Division{V}[], 1:npartitions)
    elseif ndivisions >= npartitions
        # If there are more divisions than partitions, we can distribute them
        # easily. Each partition gets 0 or more divisions.
        # TODO: Ensure usage of div here and in sampling (in PT
        # library (here), annotation, and in locations) doesn't result in 0 or
        # instead we use ceiling division
        # ndivisions_per_partition = div(ndivisions, npartitions)
        map(partition_idx->divisions[split_len(ndivisions, partition_idx, npartitions)], 1:npartitions)
    else
        # Otherwise, each division must be shared among 1 or more partitions
        allsplitdivisions = Base.Vector{Division{V}}[]
        # npartitions_per_division = div(npartitions, ndivisions)

        # Iterate through the divisions and split each of them and find the
        # one that contains a split that this partition must own and use as
        # its `partition_divisions`
        for (division_idx::Int64, division::Division{V}) in enumerate(divisions)
            # Determine the range (from `firstpartitioni` to `lastpartitioni`) of
            # partitions that own this division
            partitionsrange = split_len(npartitions, division_idx, ndivisions)

            # We need to split the division among all the partitions in
            # its range
            ndivisionsplits = length(partitionsrange)

            # Get the `Base.Vector{Number}`s to interpolate between
            divisionbegin::V = division[1]
            divisionend::V = division[2]
            T = eltype(divisionbegin)

            # Initialize divisions for each split
            # V_nonstatic = Base.Vector{T}
            splitdivisions::Base.Vector{Division{Base.Vector{T}}} =
                map(_ -> (convert(Base.Vector{T}, divisionbegin), convert(Base.Vector{T}, divisionend)), 1:ndivisionsplits)

            # Adjust the divisions for each split to interpolate. The result
            # of an `orderinghash` call can be an array (in the case of
            # strings), so we must iterate through that array in order to
            # interpolate at the first element in that array where there is a
            # difference.
            for (i::Int64, (dbegin::T, dend::T)) in enumerate(zip(divisionbegin, divisionend))
                # Find the first index in the `Base.Vector{Number}` where
                # there is a difference that we can interpolate between
                if dbegin != dend
                    # Iterate through each split
                    start::T = copy(dbegin)
                    for j::Int64 = 1:ndivisionsplits
                        # Update the start and end of the division
                        # islastsplit = j == ndivisionsplits
                        splitdivisions[j][1][i] = j == 1 ? dbegin : copy(start)
                        start += cld(dend - dbegin, ndivisionsplits)
                        start = min(start, dend)
                        splitdivisions[j][2][i] = j == ndivisionsplits ? dend : copy(start)

                        # Ensure that the remaining indices are matching between the start and end.
                        if j < ndivisionsplits
                            splitdivisions[j][2][i+1:end] = splitdivisions[j][1][i+1:end]
                        end
                    end

                    # Stop if we have found a difference we can
                    # interpolate between
                    # TODO: If the difference is not that much,
                    # interpolate between multiple consecutive
                    # differeing characters together
                    break
                end
            end

            # Each partition must have a _list_ of divisions so we must have a list
            # for each partition. So `allsplitdivisions` is an array where
            # each element is either a 1-element array containing a single
            # division or its empty.
            for splitdivision in splitdivisions
                # Check if we have already added this split division before.
                # The last split division may have been empty but we can 
                # still check whether there is a last one and if what we're
                # adding is the same or also empty. If it is the same or also
                # empty, then we just add an empty divisions list. Otherwsie,
                # we add in our novel split division.
                if !isempty(allsplitdivisions) && allsplitdivisions[end] == splitdivision
                    push!(allsplitdivisions, Division{V}[])
                else
                    push!(allsplitdivisions, Division{V}[(convert(V, splitdivision[1]), convert(V, splitdivision[2]))])
                end
            end
        end

        allsplitdivisions
    end
end

########################
# Helper MPI functions #
########################

# TODO: Fix below function for reducing values of non-equal sizes
# TODO: Make Allreducev version of below function

# function Reducev(value, op, comm::MPI.Comm)
#     # Reduces values on all processes to a single value on rank 0.
#     # 
#     # This function does the same thing as the function MPI_Reduce using
#     # only MPI_Send and MPI_Recv. As shown, it operates with additions on
#     # integers, so you could trivially use MPI_Reduce, but for operations
#     # on variable size structs for which you cannot define an MPI_Datatype,
#     # you can still use this method, by modifying it to use your op
#     # and your datastructure.

#     # TODO: Actually determine buffer
#     tag = 0
#     size = get_nworkers(comm)
#     rank = get_worker_idx(comm)-1
#     lastpower = 1 << log2(size)

#     # each of the ranks greater than the last power of 2 less than size
#     # need to downshift their data, since the binary tree reduction below
#     # only works when N is a power of two.
#     for i in lastpower:(size-1)
#         if rank == i
#             MPI.send(value, i-lastpower, tag, comm)
#         end
#     for i in 0:(size-lastpower-1)
#         if rank == i
#             MPI.Recv!(recvbuffer, i+lastpower, tag, comm)
#             value = op(value, recvbuffer)
#         end
#     end

#     for d in 0:(fastlog2(lastpower)-1)
#         k = 0
#         while k < lastpower
#             k += 1 << (d + 1)
#         end
#         receiver = k
#         sender = k + (1 << d)
#         if rank == receiver
#             MPI.Recv!(recvbuffer, 1, sender, tag)
#             value = op(value, recvbuffer)
#         elseif rank == sender
#             MPI.Send(value, 1, receiver, tag)
#         end
#     end
#     value
# end

# function fastlog2(v::UInt32)
#     multiply_de_bruijn_bit_position::Base.Vector{Int32} = [
#         0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
#         8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
#     ]

#     v |= v >> 1 # first round down to one less than a power of 2 
#     v |= v >> 2
#     v |= v >> 4
#     v |= v >> 8
#     v |= v >> 16

#     # TODO: Fix this
#     multiply_de_bruijn_bit_position[(UInt32(v * 0x07C4ACDDU) >> 27) + 1]
# end

# function tobuf(obj)::Tuple{Symbol, MPI.Buffer}
# function tobuf(obj)
#     # We pass around Julia objects between MPI processes in different ways
#     # depending on the data type. For simple isbitstype data we keep it as-is
#     # and use the simple C-like data layout for fast transfer. For dataframes,
#     # we use Arrow data layout for zero-copy deserialization. For everything
#     # else including variably-sized arrays and arbitrary Julia objects, we
#     # simply serialize and deserialize using the Serialization module in Julia
#     # standard library.

#     if isbits(obj)
#         (:bits, MPI.Buffer(Ref(obj)))
#         # (:bits, MPI.Buffer(obj))
#         # (:bits, MPI.Buffer(Ref(obj)))
#     elseif isa_array(obj) && isbitstype(first(typeof(obj).parameters)) && ndims(obj) == 1
#         # (:bits, MPI.Buffer(obj))
#         (:bits, MPI.Buffer(obj))
#     elseif isa_df(obj)
#         io = IOBuffer()
#         Arrow.write(io, obj)
#         # (:df, MPI.Buffer(view(io.data, 1:position(io))))
#         (:df, MPI.Buffer(view(io.data, 1:io.size)))
#     else
#         io = IOBuffer()
#         serialize(io, obj)
#         (:unknown, MPI.Buffer(view(io.data, 1:io.size)))
#         # (:unknown, io)
#     end
# end

function buftovbuf(buf::MPI.Buffer, comm::MPI.Comm)::MPI.VBuffer
    # This function expects that the given buf has buf.data being an array.
    # Basically what it does is it takes the result of a call to tobuf above
    # on each process and constructs a VBuffer with the sum of the sizes of the
    # buffers on different processes.
    sizes = MPI.Allgather(buf.count, comm)
    # NOTE: This function should only be used for variably-sized buffers for
    # receiving data because the returned buffer contains zeroed-out memory.
    VBuffer(similar(buf.data, sum(sizes)), sizes)
end

# function bufstosendvbuf(bufs::Base.Vector{MPI.Buffer}, comm::MPI.Comm)::MPI.VBuffer
#     sizes = [length(buf.data) for buf in bufs]
#     VBuffer(vcat(map(buf -> buf.data, bufs)), sizes)
# end

# function bufstorecvvbuf(bufs::Base.Vector{MPI.Buffer}, comm::MPI.Comm)::MPI.VBuffer
#     # This function expects that each given buf has buf.data being an array and
#     # that the number of bufs in bufs is equal to the size of the communicator.
#     # sizes = MPI.Allgather(length(buf.data), comm)
#     sizes = MPI.Alltoall([length(buf.data) for buf in bufs])
#     # NOTE: Ensure that the data fields of the bufs are initialized to have the
#     # right data type (e.g., Base.Vector{UInt8} or Base.Vector{Int64})
#     # We use `similar` here because we want zeroed out memory to receive data.
#     VBuffer(similar(first(bufs).data, sum(sizes)), sizes)
# end

# function frombuf(kind, obj)
#     if kind == :bits && obj isa Ref
#         # TODO: Ensure that the "dereferece" here is necessary
#         obj[]
#     elseif kind == :bits
#         obj
#     elseif kind == :df
#         DataFrames.DataFrame(Arrow.Table(obj), copycols = false)
#     else
#         deserialize(obj)
#     end
# end

function getpath(path::String)::String
    if startswith(path, "http://") || startswith(path, "https://")
        # TODO: First check for size of file and only download to
        # disk if it doesn't fit in free memory
        # TODO: Add option for Internet locations as to whether or not to
        # cache on disk
        hashed_path = get_remotepath_id(path)
        joined_path = "efs/job_$(Banyan.get_session().resource_id)_dataset_$(hashed_path)_$(MPI.Comm_rank(MPI.COMM_WORLD))"
        # @info "Downloading $path to $joined_path"
        # if MPI.Comm_rank(comm) == 0
        if !isfile(joined_path)
        # NOTE: Even though we are storing in /tmp, this is
        # effectively caching the download. If this is undesirable
        # to a user, a short-term solution is to use a different
        # URL each time (e.g., add a dummy query to the end of the
        # URL)
            for i = 1:3
                try
                    Downloads.download(path, joined_path)
                    break
                catch e
                    if i == 3
                        throw(e)
                    else
                        continue
                    end
                end
            end
        end
        # end
        # MPI.Barrier(comm)
        joined_path
    elseif startswith(path, "s3://")
        replace(path, "s3://" => "/home/ec2-user/s3/")
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else
        # Case of local paths to things stored on disk
        "efs/"*path
    end
end

not_is_empty(piece) = !(piece isa Empty)