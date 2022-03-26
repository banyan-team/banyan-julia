# I think it's actually _good_ for this file to be check in. It's good
# to see in each commit what was being investigated.

global investigating = Dict(
    :memory_usage => false,
    :caching => Dict(
        :location_info => false,
        :samples => false
    ),
    # Sometimes we get a smaller aggregation result or a size is smaller than
    # expected and it seems like some data is getting lost in the process of
    # PFs operating on data.
    :losing_data => false,
    :setup_nyc_taxi_stress_test => false,
    :size_exaggurated_tests => false,
    :tasks => true,
    :code_execution => Dict(
        :finishing => false
    ),
    :parallel_hdf5 => false
)

# TODO: Make this function get generated at compile-tiem and always evaluate
# to false unless BANYAN_TESTING=1

isinvestigating() = begin
    global investigating
    investigating
end