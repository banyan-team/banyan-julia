using ReTest
using Banyan, BanyanArrays
using FilePathsBase, AWSS3, HDF5

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
                nworkers = parse(Int32, get(ENV, "BANYAN_NWORKERS", "2")),
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
                force_update_files=get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? false : true,
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
                release_resources_after = get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? 5 : 0,
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

global data_for_testing = false

function use_data(data_src = "S3")
    global data_for_testing

    println("In use_data with data_src=$data_src and data_for_testing=$data_for_testing")

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
            println("In use_data with f=$f")
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
        println("In use_data force removing s3://$(get_cluster_s3_bucket_name())")
        # TODO: Maybe fsync here so that the directory gets properly updated
    end

    data_for_testing = true
end

include("hdf5.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy sessions to clean up
    end_all_sessions_for_testing()
end