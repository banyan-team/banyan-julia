module Banyan

global BANYAN_API_ENDPOINT
global SECRET_TOKEN
global AWS

export create_job,
    destroy_job,
    JobConfig,
    set_cluster_id,
    set_job_config,
    get_job_id,
    evaluate,
    record_request,
    send_request_get_response
export Future
export PartitionAnnotation,
     PartitionType,
     PartitioningConstraint,
     PartitioningConstraints,
     Partitions
export LocationType
export Task
     
export @pa, @lt
export pa_noconstraints
export Value, Block
# export Const, Mut

include("id.jl")
include("utils.jl")
include("jobs.jl")
include("locations.jl")
include("futures.jl")
include("partitions.jl")
include("queues.jl")
include("tasks.jl")
include("pa_constructors.jl")
include("pt_constructors.jl")
include("macros.jl")
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
