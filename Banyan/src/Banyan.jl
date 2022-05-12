# The Banyan client for Julia has 5 key parts:
# - Session
# - Future
# - Location, src, dst, loc
# - pt, pc
# - @partitioned

module Banyan

global BANYAN_JULIA_BRANCH_NAME = "v22.02.13"
global BANYAN_JULIA_PACKAGES = String["Banyan", "BanyanArrays", "BanyanDataFrames", "BanyanImages", "BanyanONNXRunTime", "BanyanHDF5"]
global NOT_USING_MODULES = String["ProfileView", "SnoopCompileCore"]
@time begin
using FilePathsBase: joinpath, isempty
using Base: notnothing, env_project_file

using AWSCore
using AWSS3
using AWSSQS
using Base64
using Dates
using DataStructures
using Downloads
using JSON
using Random
using Serialization
using TOML

using FileIO
using FilePathsBase
using IniFile

# For PFs:
using Serialization, Base64, MPI

# For loading
using ProgressMeter

# For testing utils
using LibGit2
println("Time to `using` libraries for Banyan.jl")
end

global BANYAN_API_ENDPOINT

# Account management
export configure

# Cluster management
export Cluster,
    create_cluster,
    update_cluster,
    destroy_cluster,
    delete_cluster,
    get_clusters,
    get_running_clusters,
    get_cluster,
    get_cluster_status,
    get_cluster_s3_bucket_name,
    assert_cluster_is_ready,
    wait_for_cluster,
    upload_to_s3

# Session management
export Session,
    start_session,
    end_session,
    end_all_sessions,
    get_session_status,
    set_session,
    get_session,
    get_session_id,
    get_sessions,
    get_running_sessions,
    get_cluster_name,
    wait_for_session,
    with_session,
    run_session,
    download_session_logs

# Futures
export AbstractFuture, Future, partitioned_computation, compute_inplace, compute

# Samples
export Sample, ExactSample, sample, sample_for_grouping, SampleForGrouping, setsample!
export sample_memory_usage,
    total_memory_usage,
    sample_axes,
    sample_keys,
    sample_by_key
    # sample_divisions,
    # sample_percentile,
    # sample_max_ngroups,
    # sample_min,
    # sample_max,
    # get_sample_computation_cache,
    # SampleComputationCache,
    # insert_in_sample_computation_cache,
    # get_key_for_sample_computation_cache
export NOTHING_SAMPLE

# Locations
export Location, LocationSource, LocationDestination, located, sourced, destined
export Value, Size, Client, Disk, None, RemoteSource, RemoteDestination
export invalidate_all_locations, invalidate_metadata, invalidate_sample
export NOTHING_LOCATION
export has_separate_metadata, get_sample, get_metadata, get_sample_and_metadata
export get_remotepath_id, get_meta_path, get_location_path, get_cached_location, cache_location, get_max_exact_sample_length, set_max_exact_sample_length

# Serialization
export from_jl_value_contents, to_jl_value_contents

# Queues
export receive_from_client, send_to_client, get_sqs_dict_from_url

# Partition types
export PartitionType, pt, pc, mutated, @partitioned
export Any,
    Replicating,
    Replicated,
    Divided,
    Syncing,
    Reducing,
    ReducingWithKey,
    Distributing,
    ScaledBySame,
    Drifted,
    Balanced,
    Unbalanced,
    Distributed,
    Blocked,
    BlockedAlong,
    Grouped,
    GroupedBy,
    Partitioned

# Partitioning constraints
export Co, Cross, Equal, Sequential, Match, MatchOn, AtMost, Scale

# Annotations
export partitioned_with,
    keep_all_sample_keys,
    keep_all_sample_keys_renamed,
    keep_sample_keys_named,
    keep_sample_keys,
    keep_sample_rate,
    partitioned_using_modules

# Debugging, PFs
export is_debug_on,
    get_s3fs_bucket_path,
    get_s3_bucket_path,
    download_remote_path,
    get_downloaded_path,
    destroy_downloaded_path,
    use_downloaded_path_for_writing,
    configure_scheduling,
    orderinghash,
    get_worker_idx,
    get_nworkers,
    is_main_worker,
    split_across,
    reduce_across,
    reduce_and_sync_across,
    sync_across,
    gather_across,
    find_worker_idx_where,
    get_partition_idx,
    get_npartitions,
    split_len,
    split_on_executor,
    merge_on_executor,
    get_partition_idx_from_divisions,
    isoverlapping,
    to_jl_value,
    to_jl_value_contents,
    from_jl_value_contents,
    get_divisions,
    getpath,
    buftovbuf,
    indexapply,
    PartiallyMerged,
    isinvestigating,
    Division,
    isnotempty,
    EMPTY_DICT

# Utilities for handling empty case
export
    Empty,
    EMPTY,
    nonemptytype,
    disallowempty,
    empty_handler

# Utilities for location constructors
export get_cached_location, cache_location, get_sample_from_data, sample_from_range

# Partitioning functions for usage in sessions that run on the cluster; dispatched
# based on `res/pf_dispatch_table.json`.
export ReturnNull,
    ReadGroupHelper,
    ReadGroup,
    SplitBlock,
    SplitGroup,
    Merge,
    CopyFrom,
    CopyFromValue,
    CopyFromClient,
    CopyFromJulia,
    CopyTo,
    CopyToClient,
    CopyToJulia,
    ReduceAndCopyToJulia,
    ReduceWithKeyAndCopyToJulia,
    Divide,
    DivideFromValue,
    DivideFromDisk,
    DivideFromClient,
    Reduce,
    ReduceWithKey,
    Distribute,
    DistributeAndShuffle

export offloaded

# Investigating for debugging purposes
include("investigating.jl")

# Helpers
include("id.jl")
include("utils_queues.jl")
include("queues.jl")

# Banyan.jl is intended both for usage as a client library and also for
# inclusion in the environment that is used for sessions that run on the cluster.
# When running on the cluster, partitioning functions define how to split,
# merge, and cast data between different kinds of partitioning. Partitioning
# functions get defined in Banyan.jl and included in sessions that run on clusters
# and functions get dispatched based on the `pf_dispatch_table.json`
# (originally called `pt_lib_info.json`) which is used by the scheduler behind
# the scenes.
include("utils.jl")
include("utils_pfs.jl")
include("pfs.jl")
include("utils_abstract_types.jl")

# Structs
include("sample.jl")
include("location.jl")
include("future.jl")
include("utils_partitions_structs.jl")
include("task.jl")
include("request.jl")
include("session.jl")

# Sessions
include("utils_s3fs.jl")
include("clusters.jl")
include("sessions.jl")

# Futures
include("samples.jl")
include("locations.jl")
include("futures.jl")

# Annotation
include("partitions.jl")
include("pt_lib_constructors.jl")
include("tasks.jl")
include("annotation.jl")

# Utilities
include("requests.jl")

# include("offloaded.jl")

function __init__()
    # The user must provide the following for authentication:
    # - User ID
    # - API key
    # - AWS credentials
    # - SSH key pair (used in cluster creation, not for auth)

    global BANYAN_API_ENDPOINT
    BANYAN_API_ENDPOINT = get(
        ENV,"BANYAN_API_ENDPOINT",
        "https://4whje7txc2.execute-api.us-west-2.amazonaws.com/prod/"
    )

    # Downloads settings
    global downloader
    downloader = Downloads.Downloader()
    downloader.easy_hook = (easy, info) ->
       Downloads.Curl.setopt(easy, Downloads.Curl.CURLOPT_LOW_SPEED_TIME, 40)
end

if Base.VERSION >= v"1.4.2"
    include("precompile.jl")
    # _precompile_()
    # precompile(__init__, ()) || @warn "Banyan failed to precompile `__init__`"
end

end # module
