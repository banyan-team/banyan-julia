include("pt_lib.jl")
include("queues.jl")

using MPI
using Dates
using Serialization

#################
# GLOBAL VALUES #
#################

provided = MPI.Init_thread(MPI.THREAD_MULTIPLE)

comm = MPI.COMM_WORLD
println(MPI.Comm_rank(comm))

global job_id = ARGS[1]
println(job_id)

global root = 0


####################
# HELPER FUNCTIONS #
####################
function is_main_node() return MPI.Comm_rank(comm) == root end

function execute_code(code_region) eval(Meta.parse(code_region)) end


#######################
# MAIN EXECUTION LOOP #
#######################

while true
    # Get next message from execution queue if main node and broadcast
    if is_main_node() == true
        message = get_next_execution_request()
    else
        message = Nothing
    end
    message = MPI.bcast(message, root, comm)

    print(MPI.Comm_rank(comm))
    # println(message)

    # Execute code
    println("executing code")
    code = message["code"]
    if is_main_node() == true
        println(code)
    end
    start_t = Dates.now()
    execute_code(code)
    end_t = Dates.now()
    println(MPI.Comm_rank(comm), " Execution time for code: ", end_t - start_t)

    # Send evaluation end
    if is_main_node() == true
        println("sending evaluation end")
        send_evaluation_end()
    end

    # # Process stages
    # for stage in message["stages"]
    #     println(stage["kind"])
    #     if stage["kind"] == "EVALUATION_END"
    #         MPI.Barrier(comm)
    #         if is_main_node() == true
    #             println("sending evaluation end")
    #             send_evaluation_end()
    #         end
    #     elseif stage["kind"] == "EXECUTION"
    #         println("executing code")
    #         code_region = stage["code"]
    #         if is_main_node() == true
    #             println(code_region)
    #         end
    #         start_t = Dates.now()
    #         execute_code(code_region)
    #         end_t = Dates.now()
    #         println(MPI.Comm_rank(comm), " Execution time for code: ", end_t - start_t)
    #     end
    # end
end
