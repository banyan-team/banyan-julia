# The Banyan client for Julia has 5 key parts:
# - Job
# - Future
# - Location, src, dst, loc
# - pt, pc
# - @partitioned

module Banyan

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

# Basic types
export configure
export Job, create_job, destroy_job, clear_jobs
export create_cluster,
    update_cluster, destroy_cluster, get_clusters, get_cluster
export Future, future, evaluate
export Location, src, dst, mem, val
export PartitionType, pt, pc, mut, @partitioned

# Locations
export None, CSVPath

# Partition types
export Block, BlockBalanced, Div, Replicate

# Constraints
export Co, Cross, Equal, Sequential, Match

# Annotations
export add_pa_to_union,
    reset_annotation, get_locations, get_mutated, get_pa_union

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
using CSV

# Jobs
include("id.jl")
include("utils.jl")
include("queues.jl")
include("jobs.jl")
include("clusters.jl")

# Futures
include("locations.jl")
include("futures.jl")
include("samples.jl")
include("sample_properties.jl")

# Annotation
include("partitions.jl")
include("pt_lib_constructors.jl")
include("tasks.jl")
include("annotation.jl")

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
