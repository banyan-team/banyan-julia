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
                nworkers = parse(Int64, get(ENV, "BANYAN_NWORKERS", "2")),
                sample_rate = sample_rate,
                print_logs = true,
                url = "https://github.com/banyan-team/banyan-julia.git",
                branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
                directory = "banyan-julia/BanyanDataFrames/test",
                dev_paths = [
                    "banyan-julia/Banyan",
                    "banyan-julia/BanyanArrays",
                    "banyan-julia/BanyanDataFrames"
                ],
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
                release_resources_after = get(ENV, "BANYAN_REUSE_RESOURCES", "0") == "1" ? 20 : 0,
                force_pull = get(ENV, "BANYAN_FORCE_PULL", "0") == "1",
                force_sync = get(ENV, "BANYAN_FORCE_SYNC", "0") == "1",
                force_install = get(ENV, "BANYAN_FORCE_INSTALL", "0") == "1",
                store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
                log_initialization=true
            )
        end,
    )

    # If selected session has already failed, this will throw an error.
    sessions_for_testing[session_config_hash] = get_session_id()

    # Set the maximum exact sample length
    set_max_exact_sample_length(max_exact_sample_length)

    # Force usage of S3FS if so desired
    if !isnothing(with_s3fs)
        ENV["BANYAN_USE_S3FS"] = with_s3fs ? "1" : "0"
    end

    configure_scheduling(name = scheduling_config_name)

    try
        f()
    catch
        # We will end the session if any error occurs. This is because we can't
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