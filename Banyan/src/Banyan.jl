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
# export Task
     
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

export Job, Future, Location, evaluate

using AWSCore

include("id.jl")
include("utils.jl")
include("jobs.jl")
include("locations.jl")
include("futures.jl")
include("evaluation.jl")

function __init__()
    global BANYAN_API_ENDPOINT
    global SECRET_TOKEN
    global AWS

    BANYAN_API_ENDPOINT = "https://zafyadmsl4.execute-api.us-west-2.amazonaws.com/dev/"
    # TODO: Remove secret token when we implement authentication
    SECRET_TOKEN = "banyan2020pumpkin"
    AWS = aws_config(region="us-west-2")
end

end # module
