function lt(name, parameters, total_memory_usage)
    lt = LocationType(
        name,
        name,
        parameters,
        parameters,
        total_memory_usage
    )
    return lt
end


function Client()
    return lt("Client", "Client", [], [], -1)
end

function Value(val::Any)
    return lt("Value", "Value", [val], [val], -1)
end

function HDF5(filename, path)
    return lt("HDF5", "HDF5", [filename, path], [filename, path], -1)
end