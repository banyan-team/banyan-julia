AlmostAny = Union{Nothing, String, Bool, Int64, Float64}

include("pt_lib.jl")
include("queues.jl")

using MPI
using Dates
using Serialization
using BenchmarkTools
using InteractiveUtils

#################
# GLOBAL VALUES #
#################

MPI.Init()

comm = MPI.COMM_WORLD
println(MPI.Comm_rank(comm))

global job_id = ARGS[1]
println(job_id)

global root = 0


####################
# HELPER FUNCTIONS #
####################
function is_main_node()
    MPI.Comm_rank(comm) == root
end

#######################
# MAIN EXECUTION LOOP #
#######################

# TODO: Try using a returned closure

# TODO: Maybe use let here to achieve the same goal of introducing local scope
for _ in 1:1

local data = Dict() # TODO: Make this more restrictive than Any

while true
    # Get next message from execution queue if main node and broadcast
    code = nothing
    if is_main_node()
        code = get_next_execution_request()["code"]
    end
    code = MPI.bcast(code, root, comm)

    # Debugging
    print(MPI.Comm_rank(comm))
    println("Executing code")
    if is_main_node() == true
        println(code)
    end

    # Execute code
    include_string(Main, code)
    function exec()
        for iter in 1:4
            @time begin
                exec_code(data)
            end
        end
    end
    # TODO: Un-comment for multiple evaluations
    # exec()
    # @btime begin @code_warntype exec_code($data) end samples=1 evals=1
    @btime exec_code($data) samples=1 evals=1

    # Send evaluation end
    if is_main_node()
        println("sending evaluation end")
        send_evaluation_end()
    end
end

end