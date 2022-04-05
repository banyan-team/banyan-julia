function get_remote_onnx_source(
    remotepath::String,
    remote_source::Location,
    remote_sample::Sample,
    shuffled::Bool
)::Location
    # Get the path
    p = download_remote_path(remotepath)
    p_exists = isfile(p)

    model = nothing
    nbytes = 0
    if isfile(p)
        pp = get_downloaded_path(p)
        model = ONNXRunTime.load_inference(pp)
        nbytes = Banyan.total_memory_usage(model)
        destroy_downloaded_path(pp)
    end

    loc_for_reading, metadata_for_reading = if p_exists
        (
            "Remote",
            Dict{String,Any}(
                "path" => remotepath,
                "nbytes" => nbytes,
                "format" => "onnx",
                "datatype" => "ONNX"
            ),
        )
    else
        ("None", EMPTY_DICT)
    end

    # Construct sample
    remote_sample = if isnothing(loc_for_reading) || isnothing(model)
        Sample()
    else
        s = ExactSample(model)
    end

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        ceil(Int, nbytes),
        remote_sample,
    )
end

function RemoteONNXSource(remotepath; shuffled=false, source_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    RemoteSource(
        get_remote_onnx_source,
        remotepath,
        shuffled = shuffled,
        source_invalid = source_invalid,
        sample_invalid = true,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    )
end
