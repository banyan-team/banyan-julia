const SessionId = String
const ResourceId = String
const ValueId = String
const MessageId = String

generated_value_ids = Set()
num_value_ids_issued = 0

function generate_value_id()::ValueId
    global generated_value_ids
    # v = randstring(8)
    global num_value_ids_issued
    num_value_ids_issued += 1
    v = string(num_value_ids_issued)
    push!(generated_value_ids, v)
    return v
end

generated_message_ids = Set()
num_message_ids_issued = 0

function generate_message_id()
    global generated_message_ids
    # v = randstring(8)
    global num_message_ids_issued
    num_message_ids_issued += 1
    v = "session_$(get_session_id())_message_$(string(num_message_ids_issued))"
    if MPI.Initialized()
        v = v * "_worker_$(get_worker_idx())"
    end
    push!(generated_message_ids, v)
    @show v
    return v
end

num_bang_values_issued = 0

function generate_bang_value()::String
    global num_bang_values_issued
    num_bang_values_issued += 1
    v = string(num_bang_values_issued)
    v
end

function get_num_bang_values_issued()
    global num_bang_values_issued
    num_bang_values_issued
end

function set_num_bang_values_issued(new_num_bang_values_issued)
    global num_bang_values_issued
    num_bang_values_issued = new_num_bang_values_issued
end