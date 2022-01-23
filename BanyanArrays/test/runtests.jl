using ReTest
using Banyan, BanyanArrays
using FilePathsBase, AWSS3, HDF5

global jobs_for_testing = Dict()

function destroy_all_jobs_for_testing()
    global jobs_for_testing
    for (job_config_hash, job_id) in jobs_for_testing
        end_session(job_id)
        delete!(jobs_for_testing, job_config_hash)
    end
end

function use_job_for_testing(
    f::Function;
    sample_rate = 2,
    max_exact_sample_length = 50,
    with_s3fs = nothing,
    scheduling_config_name = "default scheduling",
)
    haskey(ENV, "BANYAN_CLUSTER_NAME") || error(
        "Please specify the Banyan cluster to use for testing with the BANYAN_CLUSTER_NAME environment variable",
    )

    # This will be a more complex hash if there are more possible ways of
    # configuring a job for testing. Different sample rates are typically used
    # to test different data sizes. Stress tests may need a much greater sample
    # rate.
    job_config_hash = sample_rate

    # Set the job and create a new one if needed
    global jobs_for_testing
    set_job(
        if haskey(jobs_for_testing, job_config_hash)
            jobs_for_testing[job_config_hash]
        else
            start_session(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                sample_rate = sample_rate,
                print_logs = true,
                url = "https://github.com/banyan-team/banyan-julia.git",
                branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
                directory = "banyan-julia/BanyanArrays/test",
                dev_paths = [
                    "banyan-julia/Banyan",
                    "banyan-julia/BanyanArrays"
                ],
                code_files=["file://foo.jl"],
                # BANYAN_REUSE_RESOURCES should be 1 when the compute resources
                # for sessions being run can be reused; i.e., there is no
                # forced pulling, cloning, or installation going on. When it is
                # set to 1, we will reuse the same job for each session. When
                # set to 0, we will use a different job for each session but
                # each session will immediately release its resources so that
                # it can be used for the next session instead of giving up
                # TODO: Make it so that sessions that can't reuse existing jobs
                # will instead destroy jobs so that when it creates a new job
                # it can reuse the existing underlying resources.
                resource_release_delay = get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? 20 : 0,
                force_pull = get(ENV, "BANYAN_FORCE_CLONE", "0") == "0",
                force_clone = get(ENV, "BANYAN_FORCE_CLONE", "0") == "1",
                force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
                store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1"
            )
        end
    )
    # If selected job has already failed, this will throw an error.
    jobs_for_testing[job_config_hash] = get_job_id()

    # Set the maximum exact sample length
    ENV["BANYAN_MAX_EXACT_SAMPLE_LENGTH"] = string(max_exact_sample_length)

    # Force usage of S3FS if so desired
    if !isnothing(with_s3fs)
        ENV["BANYAN_USE_S3FS"] = with_s3fs ? "1" : "0"
    end

    configure_scheduling(name = scheduling_config_name)

    try
        f()
    catch
        # We will destroy the job if any error occurs. This is because we can't
        # properly intercept errors that happen in tests. If an error occurs,
        # the whole test suite exits and we don't have an opportunity to delete
        # stray jobs. This ensures that jobs are destroyed. In later tests sets,
        # `get_job()` is called which ensures that the job hasn't yet been
        # destroyed or failed.
        destroy_all_jobs_for_testing()
        rethrow()
        # If no errors occur, we will destroy all jobs in the `finally...` block.
    end
end

global data_for_testing = false

function use_data(data_src = "S3")
    global data_for_testing

    if !data_for_testing && data_src == "S3"
        original = h5open(
            download(
                "https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5",
            ),
        )
        with_downloaded_path_for_reading(
            joinpath(
                S3Path("s3://$(get_cluster_s3_bucket_name())", config = Banyan.get_aws_config()),
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
                S3Path("s3://$(get_cluster_s3_bucket_name())", config = Banyan.get_aws_config()),
                "fillval_copy.h5",
            ),
            force = true,
        )
        # TODO: Maybe fsync here so that the directory gets properly updated
    end

    data_for_testing = true
end

include("sample_computation.jl")
include("mapreduce.jl")
include("hdf5.jl")
include("black_scholes.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up
    destroy_all_jobs_for_testing()
end