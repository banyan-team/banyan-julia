# Iteration-related functions to add:
# - each* functions: eachslice/eachrow/eachcol, eachline, eachmatch
# - enumerate
# - map (lazy)
# - filter (lazy)
# - collect
# - zip
# - map should call collect on each given generator

# Generators
# - should be replicated and not scaled
# - is from one or more original arrays (or dataframes or other objects)
# - 

# Collecting generators
# - result memory usage is hard to determine but that will just get computed based on sample
# - result keys to be determined but things being iterated over can rarely be grouped and if 
# - result sample rate to be determined TODO
# TODO: Maybe it's okay for a function to mark the gen.from as scaled so that the sample rate is transferred

# Futures
# - First creating it
# - Using it in a later task
#   - Should be included in scaled but really its parent should then be used
#   for estimating memory usage, propagating sample properties (including
#   sample rate)
#   - It's PT should always be replicated but if others reference being from
#   this, then it should use its parent and also check whether 
#   - Generator should also have a size that can be copied when producing an
#   array TODO

# Changes:
# - Future should store viewing, future it is viewing, filtered from what it is viewing
# - In `viewing`, we should set the parent of any output views as well as whether
#   it is filtered
# - Remove all `filtered_from` usage and just get the parents instead
# - Next: actually use the `parents` and `filtered` for:
#   - PT constructors
#   - `map` implementation to accept generators but sometimes resulting # of rows is filtered
#   - Remaining implementation changes to `partitioned_using`
# - Remove `scaled_by_same_as` and just check `filtered`
    # - Make all `Future()`` constructor usage specify `parents` and `filtered`
# - `pt`` should check if the given future is a view and if it is just use Replicated
# - PT constructors should check if it is "from" a view and if it is, use the
#   parent and also be filtered from if the view is filtered from its parent
# - Modify `partitioned_using` to go through `scaled` and ensure it contains all
#   input views' parents but no output views
# - Make Generator store a size but it can also become nothing; then also make
#   Array have size that can be nothing and in that case it has to be
#   recomputed wherever it is used
# - Make Generator store a size which is simply deepcopied but it can be come
#   nothing if filtering happens. Then, any consumer of generators, should
#   check if the size is nothing and if it's nothing, it will know that
#   filtering occurred. TODO: Maybe use same way to determine whether filtering occurred for other things
# - If producing a generator, mark it as not scaled and make it be replicated
# - If consuming something that might be a generator, mark it as scaled and then
#   be sure to take into account that it might have been filtered

# TODO:
# - Modify `pt` to handle when a view is passed in
# - Modify `@partitioned` to handle when a view is passed in
# - Handle the following cases
#   - non-view to view
#   - view to view
#   - view to non-view
# - Modify PT constructors to use `get_children`, `get_parents`, and `filtered_from_parents`
# - Figure out how to handle the issue where the result of every iterator is going to be a
#   vector but the input parent could be a dataframe (bc of eachrow) and also there might be
#   weird transformations that happen and prevent us from being able to use the Grouped or Blocked
#   constructors which were intended only for filtering where data type is the same.

struct Generator
    fut::Future
    from::Future
    scale_from_how::String # "filtered" or "same"
end

function eachslice(A::Array{T,N}; dims)::Generator where {T,N}
    length(dims) == 1 || throw(ArgumentError("only single dimensions are supported"))
    dim = first(dims)
    dim <= ndims(A) || throw(DimensionMismatch("A doesn't have $dim dimensions"))

    gen = Future(viewing=true)

    partitioned_with(scaled=[A]) do 
        # Blocked PTs along dimensions _not_ being mapped along
        bpt = [bpt for bpt in Blocked(A) if !(compute(dims) isa Colon) && !(bpt.key in [compute(dims)...])]

        # balanced
        pt(A, bpt & Balanced())
        pt(res, Blocked() & Balanced(), match=A, on="key")

        # unbalanced
        pt(A, bpt & Unbalanced(scaled_by_same_as=res))
        pt(res, Unbalanced(scaled_by_same_as=A), match=A)

        # replicated
        # TODO: Determine why this MatchOn constraint is not propagating
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> Banyan.indexapply(+, a, b, index=axis) end), match=A, on="key")
        pt(A, res, res_size, f, dims, Replicated())
    end

    gen
end

# TODO: Allow map to operate on a generator