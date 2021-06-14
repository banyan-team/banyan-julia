# NOTE: This function is shared between the client library and the PT library
function indexapply(op, objs...; index::Integer=1)
    lists = [obj for obj in objs if obj isa AbstractVecOrTuple]
    length(lists) > 0 || throw(ArgumentError("Expected at least one tuple as input"))
    index = index isa Colon ? length(first(lists)) : index
    operands = [(obj isa AbstractVecOrTuple ? obj[index] : obj) for obj in objs]
    indexres = op(operands...)
    res = first(lists)
    if first(lists) isa Tuple
        res = [res...]
        res[index] = indexres
        Tuple(res)
    else
        res = copy(res)
        res[index] = indexres
    end
end