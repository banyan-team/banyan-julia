# Ensure to test both methods by running with `BANYAN_GITHUB_TOKEN` (PAT)
# and with `BANYAN_SSH_KEY_PATH` (SSH keys).

# @testset "private git repo test package" begin
#     # Testing when we are using a Project.toml in a private git repo
#     # that depends on other private git repos
#     # Start a session
#     using TestPkg
#     start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         url = "https://github.com/banyan-team/TestPkg.jl.git",
#         branch = "main",
#         directory = "TestPkg.jl",
#         force_sync = get(ENV, "BANYAN_FORCE_SYNC", "0") == "1",
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1"
#     )
#     res = offloaded() do
#         # using TestPkg
#         sin_plus_cos(45)
#     end
#     @show res
# end

@testset "private git repo use project.toml" begin
    # Testing when we are using a local Project.toml that depends on other
    # private git repos
    # Start a session 
    Pkg.activate("./envs/PrivateGitProject/")
    using TestPkg, TestPkg2
    start_session(
        cluster_name = ENV["BANYAN_CLUSTER_NAME"],
        nworkers = 2,
    )
    res = offloaded() do
        take_sin(45)
        sin_plus_cos(45)
    end
    @show res
end