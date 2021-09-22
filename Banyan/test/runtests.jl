using ReTest

include("sample_collection.jl")
include("sample_computation.jl")

if isempty(ARGS)
    runtests()
elseif length(ARGS) == 1
    runtests(Regex(first(ARGS)))
else
    error("Expected no more than a single pattern to match test set names on")
end
