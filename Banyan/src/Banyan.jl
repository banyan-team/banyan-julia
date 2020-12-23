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
    evaluate
export Future
#export PartitionAnnotation,
#     PartitionType,
#     join_with,
#     join_cross,
#     join_cross_eq,
#     bind_to,
#     bind_mut,
#     mut,
#     fuse,
#     seq, batches
# export @pa
# export None, Copy, Div, New, Value, Block
# export Level, C, R, N, CT
# export Const, Mut
# export SysInfo

include("id.jl")
include("utils.jl")
include("jobs.jl")
include("futures.jl")
include("partitions.jl")
include("queues.jl")
# include("pt_lib_constructors.jl")
#include("macros.jl")


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
