const JobId = String
const ValueId = String
const MessageId = String

function create_value_id()
	# TODO: Ensure that this works
	return string(rand(UInt64))
end

function get_message_id()
	# TODO: Ensure that this works
    return string(rand(MessageId))
end
