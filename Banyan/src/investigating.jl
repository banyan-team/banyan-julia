# I think it's actually _good_ for this file to be check in. It's good
# to see in each commit what was being investigated.

const INVESTIGATING_MEMORY_USAGE = false
const INVESTIGATING_CACHING_LOCATION_INFO = false
const INVESTIGATING_CACHING_SAMPLES = false
# Sometimes we get a smaller aggregation result or a size is smaller than
# expected and it seems like some data is getting lost in the process of
# PFs operating on data.
const INVESTIGATING_LOSING_DATA = false
const INVESTIGATING_SETUP_NYC_TAXI_STRESS_TEST = false
const INVESTIGATING_SIZE_EXAGGURATED_TESTS = false
const INVESTIGATING_TASKS = false
const INVESTIGATING_CODE_EXECUTION_FINISHING = false
const INVESTIGATING_PARALLEL_HDF5 = false
global INVESTIGATING_DIFFERENT_PARTITIONING_DIMS = false
global INVESTIGATING_REDUCING_GROUPBY = false
global INVESTIGATING_DESTROYING_FUTURES = false
global INVESTIGATING_FILE_PARTITION_PACKING = false
global INVESTIGATING_BDF_INTERNET_FILE_NOT_FOUND = false
global INVESTIGATING_COLLECTING_SAMPLES = false

function investigate_different_partitioning_dims(val=false)
    global INVESTIGATING_DIFFERENT_PARTITIONING_DIMS
    INVESTIGATING_DIFFERENT_PARTITIONING_DIMS = val
end

global times = Dict()

function record_time(key, t)
    global times
    if !haskey(times, key)
        times[key] = t
    else
        times[key] += t
    end
end

function get_time(key)
    global times
    get(times, key, 0)
end

function forget_times()
    global times
    empty!(times)
end

function display_times()
    global times
    if !isempty(times)
        display(times)
    end
end