# NOTE: This file is to be copied across the `runtests.jl` of all Banyan Julia
# projects

using Test
using Banyan
using BanyanArrays

function include_tests_to_run(args...)
    clear_jobs()
    for arg in args
        include(arg)
    end
end

get_enabled_tests() = lowercase.(ARGS)

# NOTE: For testing, please provide the following:
# - AWS_DEFAULT_PROFILE (if you don't already have the desired default AWS account)
# - BANYAN_USERNAME
# - BANYAN_USER_ID
# - BANYAN_API_KEY
# - BANYAN_CLUSTER_NAME
# - BANYAN_NWORKERS
# - BANYAN_NWORKERS_ALL
# 
# If these are not specified, we will only run tests that don't require a
# configured job to be created first.

function run_with_job(test_fn, name)
    # This function should be used for tests that need a job to be already
    # created to run. We look at environment variables for a specification for
    # how to authenticate and what cluster to run on

    username = get(ENV, "BANYAN_USERNAME", nothing)
    user_id = get(ENV, "BANYAN_USER_ID", nothing)
    api_key = get(ENV, "BANYAN_API_KEY", nothing)
    cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing)
    nworkers = get(ENV, "BANYAN_NWORKERS", nothing)

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
        if get(ENV, "BANYAN_NWORKERS_ALL", "false") == "true"
            for nworkers in [16, 8, 4, 2, 1]
                Job(
                    username = username,
                    user_id = user_id,
                    api_key = api_key,
                    cluster_name = cluster_name,
                    nworkers = parse(Int32, nworkers),
                    banyanfile_path = "file://res/Banyanfile.json",
                ) do j
                    test_fn(j)
                end
            end
        elseif !isnothing(nworkers)
            Job(
                username = username,
                api_key = api_key,
                cluster_name = cluster_name,
                nworkers = parse(Int32, nworkers),
                banyanfile_path = "file://res/Banyanfile.json",
                user_id = user_id,
            ) do j
                test_fn(j)
            end
        end
    end
end

function run(test_fn, name)
    # This function should be used for tests that test cluster/job managemnt
    # and so they only need environment variables to dictate how to
    # authenticate. These can be read in from ENV on a per-test basis.

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
        test_fn()
    end
end

include_tests_to_run("test_simple.jl")