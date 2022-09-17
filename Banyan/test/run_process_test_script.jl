@show get_worker_idx()
args = ENV["ARGS"]
@show args
res = args * offloaded(() -> "world")
@show res
ENV["RESULTS"] = res