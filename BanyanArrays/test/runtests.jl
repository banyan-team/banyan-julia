using ReTest

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
    set_job_id(
        if haskey(jobs_for_testing, job_config_hash)
            jobs_for_testing[job_config_hash]
        else
            create_job(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                banyanfile_path = "file://res/Banyanfile.json",
                sample_rate = sample_rate,
                return_logs = true,
            )
        end,
    )

    configure_scheduling(name = scheduling_config_name)
end

include("mapreduce.jl")
# include("hdf5.jl")
# include("bs.jl")

if isempty(ARGS)
    runtests()
elseif length(ARGS) == 1
    runtests(Regex(first(ARGS)))
else
    error("Expected no more than a single pattern to match test set names on")
end
