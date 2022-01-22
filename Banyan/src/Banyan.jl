# The Banyan client for Julia has 5 key parts:
# - Job
# - Future
# - Location, src, dst, loc
# - pt, pc
# - @partitioned

__precompile__()

module Banyan

global BANYAN_JULIA_BRANCH_NAME = "v21.12.19"
global BANYAN_JULIA_PACKAGES = ["Banyan", "BanyanArrays", "BanyanDataFrames"]

using FilePathsBase: joinpath, isempty
using Base: notnothing, env_project_file

using AWS: _get_ini_value
using AWSCore
using AWSS3
using AWSSQS
using Base64
using Downloads
using HTTP
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

# Job management
export Job,
    with_job,
    create_job,
    destroy_job,
    destroy_all_jobs,
    set_job,
    get_jobs,
    get_job,
    get_job_id,
    get_job_status,
    get_cluster_name,
    get_running_jobs,
    wait_for_job

# Session management
export start_session,
    end_session,
    get_session_status

# Futures
export AbstractFuture, Future, partitioned_computation, write_to_disk, collect

# Samples
export Sample, ExactSample, sample, setsample!
export sample_memory_usage,
    total_memory_usage,
    sample_axes,
    sample_keys,
    sample_divisions,
    sample_percentile,
    sample_max_ngroups,
    sample_min,
    sample_max

# Locations
export Location, LocationSource, LocationDestination, located, sourced, destined
export Value, Size, Client, Disk, None, RemoteSource, RemoteDestination
export clear_sources, clear_samples, invalidate_source, invalidate_sample

# Serialization
export from_jl_value_contents, to_jl_value_contents

# Queues
export receive_from_client, send_to_client

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
    Grouped,
    Partitioned

# Partitioning constraints
export Co, Cross, Equal, Sequential, Match, MatchOn, AtMost, Scale

# Annotations
export partitioned_using,
    partitioned_with,
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
    with_downloaded_path_for_reading,
    configure_scheduling,
    orderinghash,
    get_worker_idx,
    get_nworkers,
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
    to_vector,
    get_divisions,
    getpath,
    buftovbuf

# Partitioning functions for usage in jobs that run on the cluster; dispatched
# based on `res/pf_dispatch_table.json`.
export ReturnNull,
    ReadGroup,
    SplitBlock,
    SplitGroup,
    Consolidate,
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
    Reduce,
    ReduceWithKey,
    Rebalance,
    Distribute,
    DistributeAndShuffle

# Helpers
include("id.jl")
include("utils_queues.jl")
include("queues.jl")

# Banyan.jl is intended both for usage as a client library and also for
# inclusion in the environment that is used for jobs that run on the cluster.
# When running on the cluster, partitioning functions define how to split,
# merge, and cast data between different kinds of partitioning. Partitioning
# functions get defined in Banyan.jl and included in jobs that run on clusters
# and functions get dispatched based on the `pf_dispatch_table.json`
# (originally called `pt_lib_info.json`) which is used by the scheduler behind
# the scenes.
include("utils.jl")
include("utils_pfs.jl")
include("pfs.jl")

# Jobs
include("utils_abstract_types.jl")
include("utils_s3fs.jl")
include("clusters.jl")
include("jobs.jl")
include("sessions.jl")

# Futures
include("future.jl")
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

# Job (using locations and futures)
include("job.jl")

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

    load_config()
end

end # module
