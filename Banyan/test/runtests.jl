using Test
using Banyan

# TODO: Uncomment
#cluster_id = "banyantest"
#set_cluster_id(cluster_id)
#config = JobRequest(cluster_id, 2)
#create_job(config, make_current = true)

clear_jobs()

enabled_tests = lowercase.(ARGS)

function runtest(name, test_fn)
    if isempty(enabled_tests) || any([occursin(t, lowercase(name)) for t in enabled_tests])
        if "NWORKERS_ALL" in keys(ENV) && ENV["NWORKERS_ALL"] == "true"
            for nworkers in [16, 8, 4, 2, 1]
                j = Job("banyan", nworkers)
                test_fn(j)
            end
        else
            j = Job("banyan", parse(Int32, ENV["NWORKERS"]))
            test_fn(j)
        end
    end
end

include("test_l1_l2.jl")
include("test_l3.jl")