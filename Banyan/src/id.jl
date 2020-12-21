const ValueId = Int32
const MessageId = String
const JobId = String


function create_value_id()
	# TODO: Ensure that this works
	return rand(ValueId)
end

function get_message_id()
	# TODO: Ensure that this works
    return string(rand(MessageId))
end