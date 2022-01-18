struct InferenceSession <: AbstractFuture
    inference_session::Future
    dynamic_axis::Bool
end

Banyan.convert(::Type{Future}, is::InferenceSession) = is.inference_session

function load_inference(path; dynamic_axis::Bool=false)
    onnx_loc = RemoteONNXSource(path)
    onnx_loc.src_name == "RemoteONNX" || error("$path does not exist")
    # TODO: Use `datatype="ONNX", `
    InferenceSession(Future(source=onnx_loc), dynamic_axis)

    # (inputs, output_names=nothing) -> begin

    #     # Right now, we only support a single input and output
    #     # TODO: Support multiple inputs and outputs
    #     # TODO: Support specifying inputs as Tuple

    #     if length(inputs.keys()) > 1
    #         error("Multiple inputs not supported")
    #     end
    #     if length(output_names) > 1
    #         error("Multiple outputs not supported")
    #     end

    #     res = Future()
    #     input = Future(inputs[inputs.keys()[1]])

    #     res_size = Future()
    #     res = Array{Any,Any}(Future(), res_size)

    #     partitioned_with() do
    #         pt(res, Blocked(;along=1))
    #         pt(onnx, Replicated())
    #         pt(input, Blocked(;along=1))
    #     end

    #     @partitioned res onnx input begin
    #         res = onnx(input)
    #     end

    #     res
    # end
end

function (is::InferenceSession)(inputs, output_names=nothing)
    dynamic_axis = Future(is.dynamic_axis)
    res_size = Future()
    res = Future()

    is_sample = sample(is)
    output_names = isnothing(output_names) ? is_sample._output_names : output_names
    if !(length(is_sample._input_names) == 1 && length(is_sample._output_names) == 1 && length(inputs) == 1 && length(output_names) == 1)
        error("Currently only a single input and single output is supported")
    end

    A = first(values(inputs))
    input_name = Future(first(keys(inputs)))
    res = Future()
    output_name = first(output_names)

    partitioned_using() do
        keep_sample_rate(res, A)
    end

    partitioned_with() do
        # Blocked PTs along dimensions _not_ being mapped along
        bpt = [bpt for bpt in Blocked(A) if bpt.key == 1]

        # balanced
        pt(A, bpt & Balanced())
        pt(res, Blocked() & Balanced(), match=A, on="key")

        # unbalanced
        pt(A, bpt & Unbalanced(scaled_by_same_as=res))
        pt(res, Unbalanced(scaled_by_same_as=A), match=A)

        # replicated
        # TODO: Determine why this MatchOn constraint is not propagating
        pt(res_size, ReducingWithKey(quote axis -> (a, b) -> indexapply(+, a, b, index=axis) end), match=A, on="key")
        pt(A, res, res_size, is, dynamic_axis, input_name, Replicated())
    end

    @partitioned is dynamic_axis input_name A res res_size begin
        println("Started running ONNX model")
        if dynamic_axis
            res = first(values(is(Dict(input_name  => A))))
        else
            @show collect(2:ndims(A))
            res = Base.mapslices(arr -> first(values(is(Dict(input_name => arr)))), A, dims=collect(2:ndims(A)))
        end
        println("Finished running ONNX model")
        res_size = Base.size(res)
    end

    Dict(output_name => Array{eltype(sample(res)),ndims(sample(res))}(res, res_size))
end