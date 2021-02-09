include("pt_lib.jl")
include("queues.jl")

using MPI
using Dates
using Serialization

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

for _ in 1:1

comms_with_cart = Dict{Tuple{Int32, Int32, Int32}, MPI.Comm}()
comms_spanned = Dict{Tuple{Int32, Int32, Int32}, MPI.Comm}()
comms_not_spanned = Dict{Tuple{Int32, Int32, Int32}, MPI.Comm}()
data = Dict()

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
    for _ in 1:4
        include_string(Main, code)
        function exec()
            @time begin
                exec_code(
                    data,
                    comms_with_cart,
                    comms_spanned,
                    comms_not_spanned,
                )
            end
        end
        exec()
    end

    # Send evaluation end
    if is_main_node()
        println("sending evaluation end")
        send_evaluation_end()
    end
end

end