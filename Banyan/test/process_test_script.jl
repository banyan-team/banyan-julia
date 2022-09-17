@show Banyan.get_loaded_packages()
@show get_worker_idx()
@show haskey(ENV, "ARGS")
@show get(ENV, "ARGS", "")
@show ENV
@show offloaded(() -> "offloaded result")
ENV["RESULTS"] = get(ENV, "ARGS", "hello... (no ARGS")