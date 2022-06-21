# locations.jl

DataFrames_DataFrame_retry = retry(DataFrames.DataFrame; delays=Banyan.exponential_backoff_1s)

has_separate_metadata(::Val{:arrow}) = true
Tables_rowcount_retry = retry(Tables.rowcount; delays=Base.ExponentialBackOff(; n=5))
get_metadata(::Val{:arrow}, p)::Int64 = Tables_rowcount_retry(Arrow_Table_retry(p))
get_sample(::Val{:arrow}, p, sample_rate, len) = let rand_indices = sample_from_range(1:len, sample_rate)
    if sample_rate != 1.0 && isempty(rand_indices)
        DataFrames.DataFrame()
    else
        get_sample_from_data(DataFrames_DataFrame_retry(Arrow_Table_retry(p); copycols=false), sample_rate, rand_indices)
    end
end
get_sample_and_metadata(::Val{:arrow}, p, sample_rate) =
    let sample_df = DataFrames_DataFrame_retry(Arrow_Table_retry(p); copycols=false)
        num_rows = nrow(sample_df)
        get_sample_from_data(sample_df, sample_rate, num_rows), num_rows
    end

# pfs.jl

file_ending(::Val{:arrow}) = "arrow"

function read_file(::Val{:arrow}, path, rowrange, readrange, filerowrange)
    # rbrowrange = filerowrange.start:(filerowrange.start-1)
    # dfs = DataFrames.DataFrame[]
    # for tbl in Arrow.Stream(path)
    #     rbrowrange = (rbrowrange.stop+1):(rbrowrange.stop+Tables.rowcount(tbl))
    #     if Banyan.isoverlapping(rbrowrange, rowrange)
    #         readrange =
    #             max(rowrange.start, rbrowrange.start):min(
    #                 rowrange.stop,
    #                 rbrowrange.stop,
    #             )
    #         df = let unfiltered = DataFrames.DataFrame(tbl; copycols=false)
    #             unfiltered[
    #                 (readrange.start-rbrowrange.start+1):(readrange.stop-rbrowrange.start+1),
    #                 :,
    #             ]
    #         end
    #         push!(dfs, df)
    #     end
    # end
    # !isempty(dfs) ? vcat(dfs) : DataFrames.DataFrame()
    let unfiltered = DataFrames_DataFrame_retry(Arrow_Table_retry(path); copycols=false)
        starti = (readrange.start-filerowrange.start+1)
        endi = (readrange.stop-filerowrange.start+1)
        read_whole_file = starti == 0 && endi == filerowrange.stop
        read_whole_file ? unfiltered : unfiltered[starti:endi,:,]
    end
end

read_file(::Val{:arrow}, path) =
    DataFrames_DataFrame_retry(Arrow_Table_retry(path); copycols=false)

ReadBlockArrow = ReadBlockHelper(Val(:arrow))
ReadBlockBalancedArrow = ReadBlockArrow
ReadGroupHelperArrow = ReadGroupHelper(ReadBlockArrow, ShuffleDataFrame)
ReadGroupArrow = ReadGroup(ReadGroupHelperArrow)

write_file(::Val{:arrow}, part::DataFrames.DataFrame, path, nrows) =
    Arrow.write(path, part, compress=:zstd)

WriteArrow = WriteHelper(Val(:arrow))

CopyFromArrow(
    src,
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)::DataFrames.DataFrame = begin
    @time "MPI.Barrier before sync_across" MPI.Barrier(comm)
    et = @elapsed begin
    @time "Time to get part::DataFrames.DataFrame" begin
    # part::DataFrames.DataFrame = if is_main_worker(comm)
    #     println("At start of CopyFromArrow")
    #     # part_res = @time "CopyFromArray calling ReadBlockArray" ReadBlockArrow(src, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    #     @time "ReadBlockArrow" part_res1 = ReadBlockArrow(src, params, 1, 1, comm, loc_name, loc_params)
    #     @time "ConsolidateDataFrame" part_res = ConsolidateDataFrame(part_res1, EMPTY_DICT, EMPTY_DICT, comm)
    #     println("After ReadBlockArrow in CopyFromArrow with $(DataFrames.nrow(part_res)) rows and $(Banyan.format_bytes(Banyan.total_memory_usage(part_res)))")
    #     part_res
    # else
    #     DataFrames.DataFrame()
    # end
    part = begin
        @time "ReadBlockArrow" part_res1 = ReadBlockArrow(src, params, 1, 1, comm, loc_name, loc_params)
        @time "ConsolidateDataFrame" part_res = ConsolidateDataFrame(part_res1, EMPTY_DICT, EMPTY_DICT, comm)
        println("After ReadBlockArrow in CopyFromArrow with $(DataFrames.nrow(part_res)) rows and $(Banyan.format_bytes(Banyan.total_memory_usage(part_res)))")
        part_res
    end
    end
    @time "sync_across" part_synced = sync_across(empty(part), comm=comm)
    println("After sync_across in CopyFromArrow on get_worker_idx(comm)=$(get_worker_idx(comm)) and get_worker_idx()=$(get_worker_idx())")
    end
    record_time(loc_name == "Disk" ? :CopyFromArrowOnDisk : :CopyFromArrowRemote, et)
    part_synced
    
end

function CopyToArrow(
    src,
    part::Union{DataFrames.AbstractDataFrame,Empty},
    params::Dict{String,Any},
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name::String,
    loc_params::Dict{String,Any},
)
    if Banyan.get_partition_idx(batch_idx, nbatches, comm) == 1
        params["key"] = 1
        WriteArrow(src, part, params, 1, 1, MPI.COMM_SELF, loc_name, loc_params)
    end
    if batch_idx == 1
        MPI.Barrier(comm)
    end
end

# df.jl

read_arrow(p; kwargs...) = read_table(p; kwargs...)
write_arrow(A, p; kwargs...) = write_table(A, p; kwargs...)