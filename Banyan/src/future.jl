mutable struct Future <: AbstractFuture
    value::Any
    value_id::ValueId
    mutated::Bool
    stale::Bool
end