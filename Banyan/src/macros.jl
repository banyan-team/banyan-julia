macro pa(ex...)
        print(dump(ex))
	variables = ex[1].args[1].args
	constraints = ex[3]  # TODO: Check if ex[2] is wh
	code = ex[end]
	println("start variables ", variables)

	parsing_code = quote
		value_names = Dict()
		effects = Dict()
		partitions = Partitions(Dict())
	end

	i = 1
	while i < size(variables, 1) - 1
		if string(variables[i]) == "mut"
			v = variables[i + 1]
			pt = variables[i + 2]
			parsing_code = quote
				$parsing_code
				fut = $(esc(v))
				pt = $(esc(pt))
				fut.mutated = true
				#print("HERE", fut, pt)
				effects[fut.value_id] = "Mut"
				value_names[fut.value_id] = $(string(v))
				if !haskey(partitions.pt_stacks, fut.value_id)
					partitions.pt_stacks[fut.value_id] = []
				end
				push!(partitions.pt_stacks[fut.value_id], pt)
				println("partitions: ", partitions)
			end
			i += 3
		else
			v = variables[i]
			pt = variables[i + 1]
			parsing_code = quote
				$parsing_code
				fut = $(esc(v))
				pt = $(esc(pt))
				#print("HERE", fut, pt)
				effects[fut.value_id] = "Const"
				value_names[fut.value_id] = $(string(v))
				if !haskey(partitions.pt_stacks, fut.value_id)
					partitions.pt_stacks[fut.value_id] = []
				end
				push!(pt_stacks[fut.value_id], pt)
				println("partitions: ", partitions)
			end
			i += 2
		end
	end

	return quote

		constraints = $(esc(constraints))
		code = $(string(code))

		$parsing_code

		constraints = PartitioningConstraints(Set(constraints))
		pa_union = Set([PartitionAnnotation(partitions, constraints)])
		global locations

		task = Task(
			code,
			value_names,
			locations,
			effects,
			pa_union
		)

		# TODO: Add to requests_list?? no
		println("value names: ", value_names)
		println("effects: ", effects)
		println("code: ", code)
		task

	end
end

# TODO: Rename this vv
macro pp(ex...)
	tasks = ex[1]
	code = ex[end]
	#println("tasks", tasks)
	#println("code", code)
	return quote

		tasks = $(esc(tasks))
		code = $(string(code))

		pas = vcat([collect(task.pa_union) for task in tasks]...)
		global locations

		task = Task(
			code,
			merge([task.value_names for task in tasks]...),
			locations,
			merge([task.effects for task in tasks]...),
			Set(pas)
		)

		# Record request to record code region
		record_request(RecordTaskRequest(task))	
		task
	end
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
