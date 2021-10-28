using BanyanDataFrames
using BanyanArrays
using Banyan
using ReTest
using FilePathsBase, AWSS3, DataFrames, CSV, Parquet, Arrow
using LibGit2
using Random

global jobs_for_testing = Dict()

function destroy_all_jobs_for_testing()
    global jobs_for_testing
    for (job_config_hash, job_id) in jobs_for_testing
        destroy_job(job_id)
        delete!(jobs_for_testing, job_config_hash)
    end
end

function get_branch_name()
    prepo = LibGit2.GitRepo(realpath(joinpath(@__DIR__, "../..")))
    phead = LibGit2.head(prepo)
    branchname = LibGit2.shortname(phead)
    @info "Running tests with banyan-julia repository checked out to branch $branchname"
    branchname
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
            create_job(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                sample_rate = sample_rate,
                print_logs = true,
                url = "https://github.com/banyan-team/banyan-julia.git",
                branch = get(ENV, "BANYAN_JULIA_BRANCH", get_branch_name()),
                directory = "banyan-julia/BanyanDataFrames/test",
                dev_paths = [
                    "banyan-julia/Banyan",
                    "banyan-julia/BanyanArrays",
                    "banyan-julia/BanyanDataFrames"
                ],
                force_pull = get(ENV, "BANYAN_FORCE_CLONE", "0") == "0",
                force_clone = get(ENV, "BANYAN_FORCE_CLONE", "0") == "1",
                force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
                store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1"
            )
        end,
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

include("utils_data.jl")
include("groupby_filter_indexing.jl")
include("test_jobs.jl")

function use_data(file_extension, remote_kind, single_file)
    # TODO: Handle case where file_extension == "hdf5" and single_file=false
    file_extension_is_hdf5 =
        file_extension == "h5" || file_extension == "hdf5" || file_extension == "hdf"
    file_extension_is_table =
        file_extension == "csv" || file_extension == "parquet" || file_extension == "arrow"

    if file_extension_is_hdf5 && !single_file
        error("HDF5 datasets must be a single file")
    end

    # Determine URL
    url = if file_extension_is_hdf5
        "https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5"
    elseif file_extension_is_table
        "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
    else
        error("Unsupported file extension: $file_extension")
    end

    # Return the path to be passed into a read_* function
    if remote_kind == "Internet"
        url * (file_extension_is_hdf5 ? "/DS1" : "")
    elseif remote_kind == "Disk"
        # Get names and paths
        testing_dataset_local_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") * ".$file_extension"
        testing_dataset_local_path =
            joinpath(homedir(), ".banyan", "testing_datasets", testing_dataset_local_name)

        # Download if not already download
        if !isfile(testing_dataset_local_path)
            # Download to local ~/.banyan/testing_datasets
            mkpath(joinpath(homedir(), ".banyan", "testing_datasets"))
            download(url, testing_dataset_local_path)

            # Convert file if needed
            if file_extension == "parquet"
                df = CSV.read(testing_dataset_local_path, DataFrame)
                write_parquet(testing_dataset_local_path, df)
            elseif file_extension == "arrow"
                df = CSV.read(testing_dataset_local_path, DataFrame)
                Arrow.write(testing_dataset_local_path, df)
            end
        end

        testing_dataset_local_path
    elseif remote_kind == "S3"
        # Get names and paths
        testing_dataset_local_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") * ".$file_extension"
        testing_dataset_local_path =
            joinpath(homedir(), ".banyan", "testing_datasets", testing_dataset_local_name)
        testing_dataset_s3_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") *
            "_in_a_" *
            (single_file ? "file" : "dir") *
            ".$file_extension"
        testing_dataset_s3_path = S3Path(
            "s3://$(get_cluster_s3_bucket_name())/$testing_dataset_s3_name",
            config = Banyan.get_aws_config(),
        )

        # Create the file if not already created
        if !ispath(testing_dataset_s3_path)
            # Download if not already download
            if !isfile(testing_dataset_local_path)
                # Download to local ~/.banyan/testing_datasets
                mkpath(joinpath(homedir(), ".banyan", "testing_datasets"))
                download(url, testing_dataset_local_path)

                # Convert file if needed
                if file_extension == "parquet"
                    df = CSV.read(testing_dataset_local_path, DataFrame)
                    write_parquet(testing_dataset_local_path, df)
                elseif file_extension == "arrow"
                    df = CSV.read(testing_dataset_local_path, DataFrame)
                    Arrow.write(testing_dataset_local_path, df)
                end
            end

            # Upload to S3
            if single_file
                cp(Path(testing_dataset_local_path), testing_dataset_s3_path)
            else
                for i = 0:9
                    cp(
                        Path(testing_dataset_local_path),
                        joinpath(testing_dataset_s3_path, "part_$i.$file_extension"),
                    )
                end
            end
        end

        string(testing_dataset_s3_path) * (file_extension_is_hdf5 ? "/DS1" : "")
    else
        error("Unsupported kind of remote: $remote_kind")
    end
end

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

function use_empty_data()
    cleanup_tests()
    setup_basic_tests()
    setup_empty_tests()
end

include("sample_computation.jl")
include("groupby_filter_indexing.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up
    destroy_all_jobs_for_testing()
end