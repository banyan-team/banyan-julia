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

        # TODO: Change back to Remote and then specify locations to specify that format parameter is important in dispatching in the
                # PF dispatch table entry
        loc_for_reading, metadata_for_reading = if p_exists
            (
                "Remote",
                Dict(
                    "path" => remotepath,
                    "format" => "onnx",
                    "datatype" => "ONNX"
                ),
            )
        else
            ("None", Dict{String,Any}())
        end

         # Construct sample
        remote_sample = if isnothing(loc_for_reading)
            Sample()
        else
            s = nothing
            with_downloaded_path_for_reading(p) do pp
                s = ExactSample(load_inference(pp))
            end
            s
        end

        # Construct location with metadata
        LocationSource(
            loc_for_reading,
            metadata_for_reading,
            remote_sample,
        )

    end
end
