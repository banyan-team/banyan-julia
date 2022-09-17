@testset "Running process with print_logs=$print_logs and store_logs_in_s3=$store_logs_in_s3 and log_initialization=$log_initialization" for 
    print_logs in [true, false],
    (store_logs_in_s3, log_initialization) in [(false, false), (true, false), (true, true)]
    
    create_process(
        "test-process-for-running",
        "run_process_test_script.jl",
        nworkers=2,
        cluster_name=ENV["BANYAN_CLUSTER_NAME"],
        disk_capacity="auto",
        store_logs_on_cluster=true,
        force_sync=true,
        url="https://github.com/banyan-team/banyan-julia.git",
        branch=get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
        directory="banyan-julia/Banyan/test",
        dev_paths=["banyan-julia/Banyan"],
        force_pull=true,
        force_install=true
    )
    
    result = run_process("test-process-for-running", "hello ")
    @test result == "hello world"
end