function RemoteONNXSource(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    RemoteSource(
        remotepath,
        shuffled = shuffled,
        source_invalid = source_invalid,
        sample_invalid = sample_invalid,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    ) do remotepath, remote_source, remote_sample, shuffled

        # Get the path
        p = download_remote_path(remotepath)
        p_exists = isfile(p)

        model = nothing
        nbytes = 0
        if isfile(p)
            with_downloaded_path_for_reading(p) do pp
                model = ONNXRunTime.load_inference(pp)
                nbytes = Banyan.total_memory_usage(model)
            end
        end

        loc_for_reading, metadata_for_reading = if p_exists
            (
                "Remote",
                Dict(
                    "path" => remotepath,
                    "nbytes" => nbytes,
                    "format" => "onnx",
                    "datatype" => "ONNX"
                ),
            )
        else
            ("None", Dict{String,Any}())
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
end
