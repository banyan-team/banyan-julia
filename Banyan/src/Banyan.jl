# The Banyan client for Julia has 5 key parts:
# - Job
# - Future
# - Location, src, dst, loc
# - pt, pc
# - @partitioned

module Banyan

using FilePathsBase: joinpath, isempty
using Base: notnothing, env_project_file
global BANYAN_API_ENDPOINT

# TODO: Remove this
# export create_job,
#     destroy_job,
#     JobRequest,
#     set_cluster_id,
#     set_job_request,
#     get_job_id,
#     evaluate,
#     record_request,
#     send_request_get_response
# export Future
# export PartitionAnnotation,
#      PartitionType,
#      PartitioningConstraint,
#      PartitioningConstraints,
#      Partitions
# export LocationType
# export DelayedTask

# export @pa, @pp, @lt, @src, @dst
# export pa_noconstraints
# export Div, Block, Stencil
# export HDF5, Value, Client
# export Cross
# # export Const, Mut

# include("id.jl")
# include("utils.jl")
# include("jobs.jl")
# include("locations.jl")
# include("futures.jl")
# include("partitions.jl")
# include("queues.jl")
# include("tasks.jl")
# include("pa_constructors.jl")
# include("pt_constructors.jl")
# include("lt_constructors.jl")
# include("constraint_constructors.jl")
# include("macros.jl")
# include("evaluation.jl")

# Account management
export configure

# Cluster management
export Cluster,
    create_cluster,
    update_cluster,
    destroy_cluster,
    get_clusters,
    get_running_clusters,
    get_cluster,
    get_cluster_s3_bucket_name,
    assert_cluster_is_ready

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
    get_cluster_name,
    get_running_jobs

# Futures
export AbstractFuture, Future, partitioned_computation, write_to_disk, collect

# Samples
export Sample, ExactSample, sample, setsample!
export sample_memory_usage,
    sample_axes,
    sample_keys,
    sample_divisions,
    sample_percentile,
    sample_max_ngroups,
    sample_min,
    sample_max

# Locations
export Location, LocationSource, LocationDestination, located, sourced, destined
export Value, Size, Client, Disk, None, Remote
export clear_locations, clear_samples, invalidate_location, invalidate_sample

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
export Co, Cross, Equal, Sequential, Match, MatchOn, AtMost, ScaleBy

# Annotations
export partitioned_using,
    partitioned_with,
    keep_all_sample_keys,
    keep_all_sample_keys_renamed,
    keep_sample_keys_named,
    keep_sample_keys,
    keep_sample_rate

# Debugging
export is_debug_on,
    get_s3fs_bucket_path,
    get_s3_bucket_path,
    with_downloaded_path_for_reading,
    configure_scheduling,
    orderinghash

# Partitioning functions for usage in jobs that run on the cluster; dispatched
# based on `res/pf_dispatch_table.json`.
export ReturnNull,
    ReadBlock,
    ReadGroup,
    Write,
    SplitBlock,
    SplitGroup,
    Merge,
    CopyFrom,
    CopyTo,
    ReduceAndCopyTo,
    ReduceWithKeyAndCopyTo,
    Divide,
    Reduce,
    ReduceWithKey,
    Rebalance,
    Distribute,
    Consolidate,
    DistributeAndShuffle,
    Shuffle

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

using IterTools

# TODO: Move locations, samples, and parts of pt_lib.jl and pt_lib_info.json
# into their respective libraries where they can be specialized
using HDF5, CSV, Parquet, Arrow, DataFrames

# Helpers
include("id.jl")
include("queues.jl")

# Banyan.jl is intended both for usage as a client library and also for
# inclusion in the environment that is used for jobs that run on the cluster.
# When running on the cluster, partitioning functions define how to split,
# merge, and cast data between different kinds of partitioning. Partitioning
# functions get defined in Banyan.jl and included in jobs that run on clusters
# and functions get dispatched based on the `pf_dispatch_table.json`
# (originally called `pt_lib_info.json`) which is used by the scheduler behind
# the scenes.
include("utils_pfs.jl")
include("pfs.jl")

# Jobs
include("utils.jl")
include("utils_abstract_types.jl")
include("utils_s3fs.jl")
include("clusters.jl")
include("jobs.jl")

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
    # - Username
    # - API key
    # - AWS credentials
    # - SSH key pair (used in cluster creation)

    global BANYAN_API_ENDPOINT
    BANYAN_API_ENDPOINT = "https://hcohsbhhzf.execute-api.us-west-2.amazonaws.com/dev/"

    load_config()
end

end # module
