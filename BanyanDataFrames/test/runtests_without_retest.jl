# NOTE: This file is to be copied across the `runtests.jl` of all Banyan Julia
# projects

using Test
using Banyan
using BanyanArrays
using BanyanDataFrames

using Arrow
using CSV
using DataFrames
using FilePathsBase
using Parquet
using AWSS3
using Statistics
using Random

function include_tests_to_run(args...)
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
# - BANYAN_SCHEDULING_CONFIG_ALL (defaults to "false")
# - NUM_TRIALS (defaults to 1)

# TODO: Copy the below to other Banyan projects' test suites if changes are made

username = get(ENV, "BANYAN_USERNAME", nothing)
user_id = get(ENV, "BANYAN_USER_ID", nothing)
api_key = get(ENV, "BANYAN_API_KEY", nothing)
cluster_name = get(ENV, "BANYAN_CLUSTER_NAME", nothing)
nworkers = get(ENV, "BANYAN_NWORKERS", "2")
ntrials = parse(Int, get(ENV, "BANYAN_NTRIALS", "1"))

global session_id = start_session(
    username = username,
    user_id = user_id,
    api_key = api_key,
    cluster_name = cluster_name,
    nworkers = parse(Int64, nworkers),
    banyanfile_path = "file://res/Banyanfile.json",
    return_logs = true,
    sample_rate = get(ENV, "BANYAN_TEST_WITH_STRESS", "0") == "1" ? 1024 : parse(Int64, nworkers)
)

function run_with_session(test_fn, name)
    # This function should be used for tests that need a session to be already
    # created to run. We look at environment variables for a specification for
    # how to authenticate and what cluster to run on

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
        if get(ENV, "BANYAN_NWORKERS_ALL", "false") == "true"
            for nworkers in [16, 8, 4, 2, 1]
                with_session(
                    username = username,
                    api_key = api_key,
                    cluster_name = cluster_name,
                    nworkers = parse(Int64, nworkers),
                    banyanfile_path = "file://res/Banyanfile.json",
                    user_id = user_id,
                    end_session_on_exit=false
                ) do j
                    test_fn(j)
                end
            end
        elseif !isnothing(nworkers)
            with_session(session=session, end_session_on_exit=false) do j
                for i in 1:ntrials
                    if ntrials > 1
                        @time test_fn(j)
                    else
                        test_fn(j)
                    end
                end
            end
        end
    end
end

function run(test_fn, name)
    # This function should be used for tests that test cluster/session managemnt
    # and so they only need environment variables to dictate how to
    # authenticate. These can be read in from ENV on a per-test basis.

    if isempty(get_enabled_tests()) ||
       any([occursin(t, lowercase(name)) for t in get_enabled_tests()])
        test_fn()
    end
end

function include_all_tests()
    include_tests_to_run("test_io.jl")
    include_tests_to_run("test_small_dataset.jl")
    include_tests_to_run("test_medium_dataset.jl")
    include_tests_to_run("test_stress.jl")
    include_tests_to_run("test_groupby_filter_indexing.jl")
end

# bucket - name of cluster's s3 bucket
# path - path to write file to in bucket
# download_path - either http(s) link to a file or a local Path indicating the source of the file
function verify_file_in_s3(bucket, path, download_path)
     if !s3_exists(Banyan.get_aws_config(), bucket, path)
        if typeof(download_path) == String && (startswith(download_path, "https://") || startswith(download_path, "http://"))
            Downloads.download(download_path, S3Path("s3://$(bucket)/$(path)", config=Banyan.get_aws_config()))
        else  # upload local file
            cp(Path(download_path), S3Path("s3://$(bucket)/$(path)", config=Banyan.get_aws_config()))
        end
    end
end

with_session(session=session) do s
    configure_scheduling(report_schedule=true)
    if get(ENV, "BANYAN_SCHEDULING_CONFIG_ALL", "false") == "true"
        configure_scheduling(encourage_parallelism=true, encourage_parallelism_with_batches=true)
        include_all_tests()
        configure_scheduling(encourage_parallelism=true, encourage_parallelism_with_batches=false)
        include_all_tests()
        configure_scheduling(encourage_parallelism=false, encourage_parallelism_with_batches=false)
        include_all_tests()
    else
        configure_scheduling(encourage_parallelism_with_batches=true)
        include_all_tests()
    end
end

# Requirements:
# - Name testgroups and testsets and filter and run
# - Create sample data in S3 only if needed
# - Create sessions only if needed
# - Run everything on different data and on different session configs