const LocationParameters = Dict{String,Any}

mutable struct Location
    # A location may be usable as either a source or destination for data or
    # both.

    src_name::String
    dst_name::String
    src_parameters::LocationParameters
    dst_parameters::LocationParameters
    total_memory_usage::Int64
    sample::Sample
    parameters_invalid::Bool
    sample_invalid::Bool

    # function Location(
    #     src_name::String,
    #     dst_name::String,
    #     src_parameters::Dict{String,<:Any},
    #     dst_parameters::Dict{String,<:Any},
    #     total_memory_usage::Union{Int64,Nothing} = nothing,
    #     sample::Sample = Sample(),
    # )
    #     # NOTE: A file might be None and None if it is simply to be cached on
    #     # disk and then read from
    #     # if src_name == "None" && dst_name == "None"
    #     #     error(
    #     #         "Location must either be usable as a source or as a destination for data",
    #     #     )
    #     # end

    #     new(
    #         src_name,
    #         dst_name,
    #         src_parameters,
    #         dst_parameters,
    #         total_memory_usage,
    #         sample
    #     )
    # end
end