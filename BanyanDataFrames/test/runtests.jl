using ReTest
using Banyan
using FilePathsBase

global jobs_for_testing = Dict()

function use_job_for_testing(;
    sample_rate = 2,
    scheduling_config_name = "default scheduling",
)
    haskey(ENV, "BANYAN_CLUSTER_NAME") || error(
        "Please specify the Banyan cluster to use for testing with the BANYAN_CLUSTER_NAME environment variable",
    )

    # This will be a more complex hash if there are more possible ways of
    # configuring a job for testing
    job_config_hash = sample_rate

    # Set the job and create a new one if needed
    global jobs_for_testing
    set_job(
        if haskey(jobs_for_testing, job_config_hash)
            jobs_for_testing[job_config_hash]
        else
            create_job(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                banyanfile_path = "file://res/BanyanfileDebug.json",
                sample_rate = sample_rate,
                return_logs = true,
            )
        end,
    )

    configure_scheduling(name = scheduling_config_name)
end

include("utils_data.jl")

# TODO: Break up these use_* functions to be parametrized on the data format
# and only load in the files for that format. Then, testsets in
# groupby_filter_indexing.jl can iterate over all file types

function use_basic_data()
    cleanup_tests()
    # NOTE: There might be an issue here where because of S3's eventual
    # consistency property, this causes failures in writing new files
    # that are being deleted where the delete happens after the new write.
    setup_basic_tests()
end

function use_stress_data()
    cleanup_tests()
    setup_stress_tests()
end

include("groupby_filter_indexing.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up
    for job_id in values(jobs_for_testing)
        destroy_job(job_id)
    end
end