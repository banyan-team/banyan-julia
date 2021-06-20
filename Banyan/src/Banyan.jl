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
    create_cluster, update_cluster, destroy_cluster, get_clusters, get_cluster, set_cluster_status_running

# Job management
export Job, with_job, create_job, destroy_job, destroy_all_jobs, clear_jobs, get_jobs

# Futures
export AbstractFuture, Future, compute, collect

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
export Value, Size, Client, None, Remote

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
    Blocked,
    Grouped,
    ScaledBySame,
    Drifted,
    Balanced,
    Unbalanced,
    Blocked,
    Grouped

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

using AWS: _get_ini_value
using AWSCore
using AWSS3
using AWSSQS
using Base64
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

# Jobs
include("id.jl")
include("utils.jl")
include("utils_abstract_types.jl")
include("queues.jl")
include("jobs.jl")
include("clusters.jl")

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
