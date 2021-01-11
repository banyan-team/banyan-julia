macro pa(ex...)
	variables = ex[1].args[1].args[1].args
	constraints = ex[1].args[2]
	code = ex[end]

	parsing_code = quote
		value_names = Dict()
		effects = Dict()
		partitions = Partitions(Dict())
	end

	i = 1
	while i < size(variables)[0]
		if $(string(variables[i])) == "mut"
			variable = variables[i + 1]
			pt = variables[i + 2]
			parsing_code = quote
				$parsing_code
				value_names[$(esc(variable)).value_id] = $(string(variable))
				push!(get(partitions.pt_stacks, $(esc(variable)).value_id, []), $(esc(pt)))
			end
			i += 3
		else
			variable = variables[i]
			pt = variables[i + 1]
			parsing_code = quote
				$parsing_code
				value_names[$(esc(variable)).value_id] = $(string(variable))
				push!(get(partitions.pt_stacks, $(esc(variable)).value_id, []), $(esc(pt)))
			end
			i += 2
	end

	return quote

		constraints = $(esc(constraints))
		code = $(string(code))

		pa_union = set([PartitionAnnotation(partitions, constraints)])
		global locations

		task = Task(
			code,
			value_names,
			locations,
			effects,
			pa_union
		)

		# TODO: Add to requests_list?? no

		return task

	end
end

# TODO: Rename this vv
macro pp(ex...)
	tasks = ex[1]
	code = ex[end]

	return quote(

		tasks = $(esc(tasks))

		pas = vcat([collect(task.pa_union) for task in tasks]...)
		global locations

		task = Task(
			code,
			merge([pa.value_names for pa in pas]...),
			locations,
			merge([pa.effects for pa in pas]...),
			Set(pas)
		)

		# Record request to record code region
		record_request(RecordTaskRequest(task))
		
		return task
	)
end

# macro pa(ex...)
# 	annotation = ex[1].args
# 	code_region = ex[end]

# 	variables = annotation[1:end-2]
# 	effects_dict = annotation[end-1]	
# 	pa = annotation[end]

# 	# TODO: Generate code to allow PAs and annotations to be created without
# 	# wrapping in Future
# 	# variable_conversion_code = quote end
# 	# for variable in variables
# 	#     variable_conversion_code = quote
# 	#         $variable_conversion_code
# 	#         if !isa($(esc(variable)), Future)
# 	#             @debug "Creating new future"
# 	#             $(esc(variable)) = Future($(esc(variable)))
# 	#         end
# 	#     end
# 	# end

# 	value_names_creation_code = quote
# 		value_names = Dict()
# 	end
# 	for variable in variables
# 		value_names_creation_code = quote
# 			$value_names_creation_code
# 			value_names[$(esc(variable)).value_id] = $(string(variable))
# 		end
# 	end

# 	# value_names_creation_code = quote
# 	# 	value_names = Dict()
# 	# end
# 	# for variable in variables
# 	# 	value_names_creation_code = quote
# 	# 		$value_names_creation_code
# 	# 		value_names[$(esc(variable)).value_id] = $(string(variable))
# 	# 	end
# 	# end

# 	# for variable in variables
# 	# 	variable_conversion_code = quote
# 	# 		if !isa($(string(variable)), Future)
# 	# 			$(esc(variable)) = Future($(esc(variable)))
# 	# 		end
# 	# 	end
# 	# end

# 	return quote
# 		global locations
		
# 		# Get PA
# 		pa = $(esc(pa))

# 		@debug pa

# 		# $variable_conversion_code
# 		$value_names_creation_code

# 		@debug value_names

# 		# Record mutated values for later evaluation
# 		#for (value_id, effect) in pa.effects
# 		#	if effect == Mut
# 		#		record_mut(value_id)
# 		#	end
# 		#end
# 		effects_dict = $(esc(effects_dict))
# 		effects = Dict()
# 		for (fut, effect) in effects_dict
# 			if effect == "Mut"
# 				fut.mutated = true
# 			end
# 			effects[fut.value_id] = effect
# 		end
# 		pa = Set([pa])

# 		println(effects)
# 		println($(string(code_region)))

# 		# Record request to record code region
# 		record_request(RecordTaskRequest(Task(
# 			$(string(code_region)),
# 			value_names,
# 			locations,
# 			effects,
# 			pa
# 		)))
# 	end
# end

macro lt(ex...)
	fut = ex[1]
	lt = ex[end]

	return quote
		# Get LT
		lt = $(esc(lt))

		# Get future
		fut = $(esc(fut))

		@debug lt

		fut.location_type = lt

		global locations
		locations[fut.value_id] = lt
	end

end

macro src(ex...)
	fut = ex[1]
	lt = ex[2]

	return quote
		fut = $(esc(fut))
		lt = $(esc(lt))

		fut.location_type.src_name = lt.src_name
		fut.location_type.src_parameters = lt.src_parameters
		# TODO: update memory usage?

		global locations
		locations[fut.value_id] = fut.location_type
	end

end

macro dst(fut, lt)
	fut = ex[1]
	lt = ex[2]

	return quote
		fut = $(esc(fut))
		lt = $(esc(lt))

		fut.location_type.dst_name = lt.dst_name
		fut.location_type.dst_parameters = lt.dst_parameters
		# TODO: update memory usage?

		global locations
		locations[fut.value_id] = fut.location_type
	end
end