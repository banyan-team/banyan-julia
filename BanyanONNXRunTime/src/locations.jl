function RemoteONNXSource(remotepath)::Location
    # NOTE: We can't cache this because ONNX models can't be deserialized from disk

    # Get the path
    p = download_remote_path(remotepath)
    p_exists = isfile(p)

    model = nothing
    nbytes = 0
    if p_exists
        pp = get_downloaded_path(p)
        model = ONNXRunTime.load_inference(pp)
        nbytes = Banyan.total_memory_usage(model)
        destroy_downloaded_path(pp)
    end

    p_exists || error("No ONNX file found at $remotepath")

    loc_for_reading = "Remote"
    metadata_for_reading = Dict{String,Any}(
        "path" => remotepath,
        "format" => "onnx",
        "datatype" => "ONNX"
    )

    # Construct sample
    remote_sample = ExactSample(model)

    # Construct location with metadata
    LocationSource(
        loc_for_reading,
        metadata_for_reading,
        ceil(Int64, nbytes),
        remote_sample,
    )
end
