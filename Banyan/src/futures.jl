###########
# Futures #
###########

"""
    Future()
    Future(value::Any)
    Future(location::Location)
    Future(; kwargs...)

Constructs a new future, representing a value that has not yet been evaluated.
"""
function Future(location::Location = None(); mutate_from::Union{<:AbstractFuture,Nothing}=nothing)
    # Generate new value id
    value_id = generate_value_id()

    # Create new Future and assign a location to it
    new_future = Future(nothing, value_id, false, false)
    sourced(new_future, location)
    destined(new_future, None())

    # TODO: Add Size location here if needed
    # Handle locations that have an associated value
    if location.src_name in ["None", "Client", "Value"]
        new_future.value = location.sample.value
        new_future.stale = false
    end
    
    if !isnothing(mutate_from)
        # Indicate that this future is the result of an in-place mutation of
        # some other value
        mutated(mutate_from, new_future)
    elseif location.src_name == "None"
        # For convenience, if a future is constructed with no location to
        # split from, we assume it will be mutated in the next code region
        # and mark it as mutated. This is pretty common since often when
        # we are creating new futures with None location it is as an
        # intermediate variable to store the result of some code region.
        # 
        # Mutation can also be specified manually with mutate=true|false in
        # `partition` or implicitly through `Future` constructors
        mutated(new_future)
    end

    new_future
end

function Future(value::Any)
    location = if Base.summarysize(value) ≤ 4 * 1024
        Value(value)
    else
        Client(value)
    end

    # Create future, store value, and return
    Future(location)
end

"""
    Future(future::AbstractFuture)

Constructs a future from a future that was already created.

If the given future has not had its value mutated (meaning that the value
stored with it here on the client is the most up-to-date version of it), we use
its value to construct a new future from a copy of the value.

However, if the future has been mutated by some code region that has already
been recorded, we construct a new future with location `None` and mark it as
mutated. This is because presumably in the case that we _can't_ copy over the
given future, we would want to assign to it in the upcoming code region where
it's going to be used.
"""
function Future(fut::AbstractFuture, mutation::Function=identity)
    fut = convert(Future, fut)
    if !fut.stale
        # Copy over value
        new_future = Future(
            deepcopy(mutation(fut.value)),
            generate_value_id(),
            # If the future is not stale, it is not mutated in a way where
            # a further `compute` is needed. So we can just copy its value.
            false,
            false
        )

        # Copy over location
        located(new_future, deepcopy(get_location(fut)))

        new_future
    else
        Future()
    end
end

# convert(::Type{Future}, value::Any) = Future(value)
convert(::Type{Future}, fut::Future) = fut

get_location(fut::AbstractFuture) = get(get_job().locations, convert(Future, fut).value_id, nothing)
get_location(value_id::ValueId) = get(get_job().locations, value_id, nothing)
get_future(value_id::ValueId) = get_job().futures_on_client[value_id]
