const JobId = String
const ValueId = String
const MessageId = String

generated_value_ids = Set()
num_value_ids_issued = 0

function generate_value_id()
    global generated_value_ids
    # v = randstring(8)
    global num_value_ids_issued
    num_value_ids_issued += 1
    v = string(num_value_ids_issued)
    if v in generated_value_ids
        println("Duplicate value ID: $v")
    end
    push!(generated_value_ids, v)
    return v
end

generate_message_id() = randstring(8)
