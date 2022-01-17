function load_inference(path; kwargs...)
    onnx_loc = RemoteONNXSource(path)
    onnx_loc.src_name == "Remote" || error("$path does not exist")
    onnx = Future(datatype="Onnx", source=onnx_loc)

    (inputs, output_names=nothing) -> begin

        # Right now, we only support a single input and output
        # TODO: Support multiple inputs and outputs
        # TODO: Support specifying inputs as Tuple

        if length(inputs.keys()) > 1
            error("Multiple inputs not supported")
        end
        if length(output_names) > 1
            error("Multiple outputs not supported")
        end

        res = Future()
        input = Future(inputs[inputs.keys()[1]])

        partitioned_with() do
            pt(res, Blocked(;along=1))
            pt(onnx, Replicated())
            pt(input, Blocked(;along=1))
        end

        @partitioned res onnx input begin
            res = onnx(input)
        end

        res
    end
end