# The Banyan client for Julia has 5 key parts:
# - Job
# - Future
# - Location, src, dst, loc
# - pt, pc
# - @partitioned

module Banyan

global BANYAN_API_ENDPOINT
global SECRET_TOKEN
global AWS

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
# export BTask

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
export Job, create_job, destroy_job
export Future, evaluate
export Location, src, dst, loc
export PartitionType, pt, pc, mut, @partitioned

# Locations
export None

# Partition types
export Block

# Constraints
export Co, Cross, Equals, Sequential, Matches

# Annotations
export add_pa_to_union, reset_annotation, get_locations, get_mutated, get_pa_union

using AWSCore

# Jobs
include("id.jl")
include("utils.jl")
include("queues.jl")
include("jobs.jl")

# Futures
include("locations.jl")
include("futures.jl")

# Annotation
include("partitions.jl")
include("pt_lib_constructors.jl")
include("tasks.jl")
include("annotation.jl")

function __init__()
    global BANYAN_API_ENDPOINT
    global SECRET_TOKEN
    global AWS

    BANYAN_API_ENDPOINT = "https://zafyadmsl4.execute-api.us-west-2.amazonaws.com/dev/"
    # TODO: Remove secret token when we implement authentication
    SECRET_TOKEN = "banyan2020pumpkin"
    AWS = aws_config(region = "us-west-2")
end

end # module
