# NOTE: This file is to be copied across the `runtests.jl` of all Banyan Julia
# projects

using Test
using Banyan
using BanyanArrays

function include_tests_to_run(args...)
    # TODO: Probably remove the `clear_jobs`
    clear_jobs()
    for arg in args
        include(arg)
    end
end

get_enabled_tests() = lowercase.(ARGS)

# NOTE: For testing, please provide the following:
# - AWS_DEFAULT_PROFILE (if you don't already have the desired default AWS account)
# - BANYAN_USERNAME (optional)
# - BANYAN_USER_ID (optional)
# - BANYAN_API_KEY (optional)
# - BANYAN_CLUSTER_NAME (required)
# - BANYAN_NWORKERS (defaults to 2)
# - BANYAN_NWORKERS_ALL (defaults to "false")
# - NUM_TRIALS (defaults to 1)

# TODO: Copy the below to other Banyan projects' test suites if changes are made

username = get(ENV, "BANYAN_USERNAME", nothing)
user_id = get(ENV, "BANYAN_USER_ID", nothing)
api_key = get(ENV, "BANYAN_API_KEY", nothing)
cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing)
nworkers = get(ENV, "BANYAN_NWORKERS", "2")
ntrials = parse(Int, get(ENV, "NUM_TRIALS", "1")) # TODO: Make this BANYAN_NTRIALS

global job = create_job(
    username = username,
    user_id = user_id,
    api_key = api_key,
    cluster_name = cluster_name,
    nworkers = parse(Int32, nworkers),
    banyanfile_path = "file://res/Banyanfile.json",
)

function run_with_job(test_fn, name)
    # This function should be used for tests that need a job to be already
    # created to run. We look at environment variables for a specification for
    # how to authenticate and what cluster to run on

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
        if get(ENV, "BANYAN_NWORKERS_ALL", "false") == "true"
            for nworkers in [16, 8, 4, 2, 1]
                with_job(
                    username = username,
                    api_key = api_key,
                    cluster_name = cluster_name,
                    nworkers = parse(Int32, nworkers),
                    banyanfile_path = "file://res/Banyanfile.json",
                    user_id = user_id,
                    destroy_job_on_exit=false
                ) do j
                    test_fn(j)
                end
            end
        elseif !isnothing(nworkers)
            with_job(job=job, destroy_job_on_exit=false) do j
                for i in 1:ntrials
                    if ntrials > 1
                        @time test_fn(j)
                    else
                        @time test_fn(j)
                    end
                end
            end
        end
    end
end

function run_without_job(test_fn, name)
    # This function should be used for tests that don't need a job
    # and are run locally, such as those for baselines.

    num_trials = parse(Int, get(ENV, "NUM_TRIALS", "1"))

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
       for i in 1:num_trials
            @time test_fn(1)
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

with_job(job=job) do j
    # NOTE: We need to wrap the `include`s in `with_job` because if the tests
    # fail without an error occuring, then a `LoadError` gets thrown. If an
    # error occurs inside a test, the job gets destroyed and the error is
    # rethrown and then caught here. So if an error occurs, `destroy_job` gets
    # called twice. But that's OK. And because we store
    # `jobs_destroyed_recently`, we will only submit a single call to
    # `destroy-job`.
    include_tests_to_run("test_mapreduce.jl")
    include_tests_to_run("test_hdf5.jl")
    include_tests_to_run("test_black_scholes.jl")
end
