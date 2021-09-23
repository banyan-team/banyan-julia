using ReTest
using HDF5

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
                banyanfile_path = "file://res/BanyanfileDebug.json",
                sample_rate = sample_rate,
                return_logs = true,
            )
        end,
    )

    configure_scheduling(name = scheduling_config_name)
end

global data_for_testing = false

function use_data(data_src="S3")
    global data_for_testing

    if !data_for_testing && data_src == "S3"
        original = h5open(
            download(
                "https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5",
            ),
        )
        with_downloaded_path_for_reading(
            joinpath(
                S3Path(get_cluster_s3_bucket_name(), config = Banyan.get_aws_config()),
                "fillval.h5",
            ),
            for_writing = true,
        ) do f
            new = h5open(string(f), "w")
            new["DS1"] = repeat(original["DS1"][:, :], 100, 100)
            close(new)
        end
        close(original)

        # rm(get_s3fs_path(joinpath(get_cluster_s3_bucket_name(), "fillval_copy.h5")), force=true)
        rm(
            joinpath(
                S3Path(get_cluster_s3_bucket_name(), config = Banyan.get_aws_config()),
                "fillval_copy.h5",
            ),
            force = true,
        )
        # TODO: Maybe fsync here so that the directory gets properly updated
    end

    data_for_testing = true
end

include("mapreduce.jl")
include("hdf5.jl")
include("bs.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up
    for job_id in values(jobs_for_testing)
        destroy_job(job_id)
    end
end