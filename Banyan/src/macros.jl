macro pa(ex...)
	annotation = ex[1].args
	code_region = ex[end]

	variables = annotation[1:end-1]
	pa = annotation[end]

	# TODO: Generate code to allow PAs and annotations to be created without
	# wrapping in Future
	# variable_conversion_code = quote end
	# for variable in variables
	#     variable_conversion_code = quote
	#         $variable_conversion_code
	#         if !isa($(esc(variable)), Future)
	#             @debug "Creating new future"
	#             $(esc(variable)) = Future($(esc(variable)))
	#         end
	#     end
	# end

	value_names_creation_code = quote
		value_names = Dict()
	end
	for variable in variables
		value_names_creation_code = quote
			$value_names_creation_code
			value_names[$(esc(variable)).value_id] = $(string(variable))
		end
	end

	for variable in variables
		variable_conversion_code = quote
			if !isa($(string(variable)), Future)
				$(esc(variable)) = Future($(esc(variable)))
			end
		end
	end

	return quote
		# Get PA
		pa = $(esc(pa))

		@debug pa

		# $variable_conversion_code
		$value_names_creation_code

		@debug value_names

		# Record mutated values for later evaluation
		#for (value_id, effect) in pa.effects
		#	if effect == Mut
		#		record_mut(value_id)
		#	end
		#end

		# Record request to record code region
		record_request(Task(
			$(string(code_region)),
			value_names,
			pa
		))
	end
end
