using Test
using Banyan

clear_jobs()
enabled_tests = lowercase.(ARGS)

# NOTE: For testing, please provide the following:
# - AWS_DEFAULT_PROFILE (if you don't already have the desired default AWS account)
# - BANYAN_USERNAME
# - BANYAN_API_KEY
# - BANYAN_CLUSTER_NAME
# - BANYAN_NWORKERS
# - BANYAN_NWORKERS_ALL
# 
# If these are not specified, we will only run tests that don't require a
# configured job to be created first.

function run_with_job(name, test_fn)
    # This function should be used for tests that need a job to be already
    # created to run. We look at environment variables for a specification for
    # how to authenticate and what cluster to run on

    username = get(ENV, "BANYAN_USERNAME", nothing)
    api_key = get(ENV, "BANYAN_API_KEY", nothing)
    cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing)
    nworkers = get(ENV, "BANYAN_NWORKERS", nothing)

    if isempty(enabled_tests) ||
       any([occursin(t, lowercase(name)) for t in enabled_tests])
        if get(ENV, "BANYAN_NWORKERS_ALL", "false") == "true"
            for nworkers in [16, 8, 4, 2, 1]
                j = Job(
                    username = username,
                    api_key = api_key,
                    cluster_name = cluster_name,
                    nworkers = parse(Int32, nworkers),
                    banyanfile_path = "file://res/Banyanfile.json",
                )
                test_fn(j)
                use(j)
            end
        elseif !isnothing(nworkers)
            j = Job(
                username = username,
                api_key = api_key,
                cluster_name = cluster_name,
                nworkers = parse(Int32, nworkers),
                banyanfile_path = "file://res/Banyanfile.json",
            )
            test_fn(j)
            use(j)
        end
    end
end

function run(name, test_fn)
    # This function should be used for tests that test cluster/job managemnt
    # and so they only need environment variables to dictate how to
    # authenticate. These can be read in from ENV on a per-test basis.

    if isempty(enabled_tests) ||
       any([occursin(t, lowercase(name)) for t in enabled_tests])
        test_fn()
    end
end

include("test_cluster.jl")
include("test_l1_l2.jl")
