mutable struct InferenceSession <: AbstractFuture
    inference_session::Future
    dynamic_axis::Bool
end

Banyan.convert(::Type{Future}, is::InferenceSession) = is.inference_session

function pts_for_is(futures)
    A, res, res_size, is, dynamic_axis, input_name = futures

    # Blocked PTs along dimensions _not_ being mapped along
    bpt = [bpt for bpt in Blocked(A) if bpt.parameters["key"] == 1]
            
    # balanced
    pt(A, bpt & Balanced())
    pt(res, BlockedAlong() & Balanced(), match=A, on="key")

    # unbalanced
    pt(A, bpt & Unbalanced(res))
    pt(res, Unbalanced(A), match=A)

    # replicated
    # TODO: Determine why this MatchOn constraint is not propagating
    pt(res_size, ReducingWithKey(add_sizes_on_axis), match=A, on="key")
    pt(A, res, res_size, is, dynamic_axis, input_name, Replicated())
end

function (is::InferenceSession)(inputs, output_names=nothing)
    dynamic_axis = Future(is.dynamic_axis)
    res_size = Future()
    res = Future()

    is_fut = is.inference_session
    is_sample = sample(is_fut)
    output_names = isnothing(output_names) ? is_sample._output_names : output_names
    if !(length(is_sample._input_names) == 1 && length(is_sample._output_names) == 1 && length(inputs) == 1 && length(output_names) == 1)
        error("Currently only a single input and single output is supported")
    end

    A = first(values(inputs)).data
    input_name = Future(first(keys(inputs)))
    res = Future(datatype="Array")
    output_name = first(output_names)

    partitioned_with(pts_for_is, [A, res, res_size, is_fut, dynamic_axis, input_name], scaled=[A, res], modules=["ONNXRunTime"])

    @partitioned is dynamic_axis input_name A res res_size begin
        if dynamic_axis
            res = first(values(is(Dict(input_name  => A))))
        else
            res = Base.mapslices(arr -> first(values(is(Dict(input_name => arr)))), A, dims=Base.collect(2:ndims(A)))
        end
        res_size = Base.size(res)
    end

    res_sample = sample(res)
    T = eltype(res_sample)
    N = ndims(res_sample)
    Dict{String,BanyanArrays.Array{T,N}}(output_name => BanyanArrays.Array{T,N}(res, res_size))
end

function load_inference(path; dynamic_axis::Bool=false)
    onnx_loc = RemoteONNXSource(path)
    onnx_loc.src_name == "Remote" || error("$path does not exist")
    InferenceSession(Future(source=onnx_loc, datatype="ONNX"), dynamic_axis)
end