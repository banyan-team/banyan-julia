###########
# Futures #
###########

# Every future does have a location assigned to it: both a source and a
# location. If the future is created from some remote location, the
# location will have a memory usage that will tell the future how much memory
# is used.

get_location(v::ValueId)::Location = get(get_session().locations, v, NOTHING_LOCATION)
get_location(f::Future)::Location = get(get_session().locations, f.value_id, NOTHING_LOCATION)

function create_new_future(source::Location, mutate_from::Future, datatype::String)
    # Generate new value id
    value_id::ValueId = generate_value_id()

    # Create new Future and assign a location to it
    new_future = create_future(datatype, nothing, value_id, false, true)
    sourced(new_future, source)
    destined(new_future, None())

    # TODO: Add Size location here if needed
    # Handle locations that have an associated value
    source_src_name = source.src_name
    if source_src_name == "None" || source_src_name ==  "Client" || source_src_name ==  "Value"
        new_future.value = source.sample.value
        new_future.stale = false
    end
    
    if !isnothing(mutate_from)
        # Indicate that this future is the result of an in-place mutation of
        # some other value
        mutated(mutate_from, new_future)
    elseif source.src_name == "None"
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

function create_future_from_sample(value::T, datatype::String)::Future where T
    location::Location = if total_memory_usage(value) ≤ 4 * 1024
        Value(value)
    else
        # TODO: Store values in S3 instead so that we can read from there
        Client(value)
    end

    # Create future, store value, and return
    create_new_future(location, NOTHING_FUTURE, datatype)
end

# Constructs a future from a future that was already created.

# If the given future has not had its value mutated (meaning that the value
# stored with it here on the client is the most up-to-date version of it), we use
# its value to construct a new future from a copy of the value.

# However, if the future has been mutated by some code region that has already
# been recorded, we construct a new future with location `None` and mark it as
# mutated. This is because presumably in the case that we _can't_ copy over the
# given future, we would want to assign to it in the upcoming code region where
# it's going to be used.

function create_future_from_existing(fut::Future, @nospecialize(mutation::Function))::Future
    if !fut.stale
        # Copy over value
        new_future = create_future(
            fut.datatype,
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
        create_new_future(None(), NOTHING_FUTURE, fut.datatype)
    end
end

struct NothingValue end
const NOTHING_VALUE = NothingValue()

function Future(
    value::Any = NOTHING_VALUE; datatype="Any",
    source::Location = None(), mutate_from::Union{Future,Nothing}=nothing,
    from::Union{AbstractFuture,Nothing} = nothing, @nospecialize(mutation::Function=identity),
)
    if !isnothing(from)
        create_future_from_existing(convert(Future, from)::Future, mutation)
    elseif value isa NothingValue
        create_new_future(source, isnothing(mutate_from) ? NOTHING_FUTURE : mutate_from, datatype)
    else
        create_future_from_sample(value, datatype)
    end
end