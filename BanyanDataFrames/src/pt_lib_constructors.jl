function ReducingGroupBy(groupcols, groupkwargs, args, kwargs)::Base.Vector{PartitionType}
    # We must be keeping keys so that we can groupby and combine the results
    # of the individual partitioned computations.
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show kwargs
    end
    renamecols = get(kwargs, :renamecols, true)
    if !get(kwargs, :keepkeys, true)
        return PartitionType[]
    end

    args_flattened = []
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show args
    end
    for arg in args
        if arg isa Base.AbstractVector{Pair}
            append!(args_flattened, arg)
        else
            push!(args_flattened, arg)
        end
    end
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show args_flattened
    end

    # Convert arguments to pairs
    pairs = []
    for arg in args_flattened
        if arg isa Pair
            push!(pairs, arg)
        elseif arg isa Function
            push!(pairs, nothing => arg => string(arg))
        else
            return PartitionType[]
        end
    end
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show pairs
    end
    

    # Convert arguments to triplets (e.g., `:init => (max => :final)`)
    triplets = []
    for arg in pairs
        if arg[2] isa Pair && arg[2][1] isa Function
            push!(triplets, (arg[1], arg[2][1], arg[2][2]))
        elseif arg[1] isa Function
            push!(triplets, (nothing, arg[1], arg[2]))
        elseif arg[2] isa Function
            push!(triplets, (arg[1], arg[2], renamecols ? ("$(arg[1])_$(arg[2])") : arg[1]))
        else
            return PartitionType[]
        end
    end
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show triplets
    end

    # Convert to arguments to pass into combine
    reducing_args = []
    mean_cols = []
    for (from, func, to) in triplets
        if func == DataFrames.nrow
            push!(reducing_args, to => sum => to)
        elseif func == minimum || func == maximum || func == sum
            push!(reducing_args, to => func => to)
        elseif func == mean
            push!(reducing_args, to => sum => to)
            push!(reducing_args, DataFrames.nrow => :banyan_averaging_nrow)
            push!(mean_cols, to)
        else
            return PartitionType[]
        end
    end
    if Banyan.INVESTIGATING_REDUCING_GROUPBY
        @show reducing_args
        @show mean_cols
    end

    # Create the functions
    reducing_op = (a, b) -> begin
        gdf = DataFrames.groupby(vcat(a, b), groupcols; groupkwargs...)
        DataFrames.combine(gdf, reducing_args...; kwargs...)
    end
    finishing_op = df -> begin
        for to in mean_cols
            # TODO: Determine whether ./ will work even if from and to are not just single columns
            df[!, to] = df[!, to] ./ df[!, :banyan_averaging_nrow]
        end
        if !isempty(mean_cols)
            DataFrames.select!(df, Not(:banyan_averaging_nrow))
        end
        df
    end

    # Return the partition type
    PartitionType[PartitionType("name" => "Replicating", Banyan.noscale, "replication" => nothing, "reducing_op" => to_jl_value(reducing_op), "finishing_op" => to_jl_value(finishing_op))]
end