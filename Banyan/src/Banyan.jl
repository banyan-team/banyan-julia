module Banyan

global BANYAN_API_ENDPOINT
global SECRET_TOKEN
global AWS

# export create_environment,
#     destroy_environment,
#     EnvironmentConfig,
#     set_environment_config,
#     get_environment_id,
#     record_mut,
#     evaluate
# export Future
# # export eval TODO: determine how to export this and make sure that's safe
# export PartitionAnnotation,
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

# include("id.jl")
# include("utils.jl")
# include("utils_conversion.jl")
# include("sys_info.jl")
# include("environments.jl")
# include("futures.jl")
# include("partitions.jl")
# include("pt_lib_constructors.jl")
# include("annotation.jl")


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
