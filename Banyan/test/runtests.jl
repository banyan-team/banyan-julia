using Test
using Banyan

username = "BanyanTest"
cluster_id = "banyancluster"

# Don't rerun this.
#create_cluster(cluster_id)

clear_jobs()

enabled_tests = lowercase.(ARGS)

function runtest(name, test_fn)
    if isempty(enabled_tests) || any([occursin(t, lowercase(name)) for t in enabled_tests])
        if "NWORKERS_ALL" in keys(ENV) && ENV["NWORKERS_ALL"] == "true"
            for nworkers in [16, 8, 4, 2, 1]
                j = Job(username, cluster_id, nworkers)
                test_fn(j)
	        use(j)
            end
        else
            j = Job(username, cluster_id, parse(Int32, ENV["NWORKERS"]))
            test_fn(j)
            use(j)
        end
    end
end

include("test_l1_l2.jl")
include("test_l3.jl")
