using Banyan
using ReTest
using FilePathsBase, AWSS3, DataFrames, CSV, Parquet, Arrow
using Pkg

global sessions_for_testing = Dict()

function end_all_sessions_for_testing()
    global sessions_for_testing
    for (session_config_hash, session_id) in sessions_for_testing
        end_session(session_id)
        delete!(sessions_for_testing, session_config_hash)
    end
end

function use_session_for_testing(
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
    # configuring a session for testing. Different sample rates are typically used
    # to test different data sizes. Stress tests may need a much greater sample
    # rate.
    session_config_hash = sample_rate

    # Set the session and create a new one if needed
    global sessions_for_testing
    set_session(
        if haskey(sessions_for_testing, session_config_hash)
            sessions_for_testing[session_config_hash]
        else
            start_session(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                sample_rate = sample_rate,
                print_logs = true,
                url = "https://github.com/banyan-team/banyan-julia.git",
                branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
                directory = "banyan-julia/Banyan/test",
                dev_paths = [
                    "banyan-julia/Banyan",
                ],
                # BANYAN_REUSE_RESOURCES should be 1 when the compute resources
                # for sessions being run can be reused; i.e., there is no
                # forced pulling, cloning, or installation going on. When it is
                # set to 1, we will reuse the same session for each session. When
                # set to 0, we will use a different session for each session but
                # each session will immediately release its resources so that
                # it can be used for the next session instead of giving up
                # TODO: Make it so that sessions that can't reuse existing sessions
                # will instead destroy sessions so that when it creates a new session
                # it can reuse the existing underlying resources.
                release_resources_after = get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? 20 : 0,
                force_pull = get(ENV, "BANYAN_FORCE_PULL", "0") == "1",
                force_clone = get(ENV, "BANYAN_FORCE_CLONE", "0") == "1",
                force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
                store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1"
            )
        end
    )
    # If selected session has already failed, this will throw an error.
    sessions_for_testing[session_config_hash] = get_session_id()

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
        # We will destroy the session if any error occurs. This is because we can't
        # properly intercept errors that happen in tests. If an error occurs,
        # the whole test suite exits and we don't have an opportunity to delete
        # stray sessions. This ensures that sessions are destroyed. In later tests sets,
        # `get_session()` is called which ensures that the session hasn't yet been
        # destroyed or failed.
        end_all_sessions_for_testing()
        rethrow()
        # If no errors occur, we will destroy all sessions in the `finally...` block.
    end
end

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
        if single_file ? !ispath(testing_dataset_s3_path) : !ispath(joinpath(testing_dataset_s3_path, "part_0.$file_extension"),)
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

include("sample_computation.jl")
include("config.jl")
include("clusters.jl")
include("sessions.jl")
include("offloaded.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # End sessions to clean up.
    end_all_sessions_for_testing()
end