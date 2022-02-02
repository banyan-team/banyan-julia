function SetIntraOpNumThreads(
    api::OrtApi,
    options::OrtSessionOptions,
    intra_op_num_threads::Integer,
)
    status = @ccall $(api.SetIntraOpNumThreads)(
        options.ptr::Ptr{Cvoid},
        intra_op_num_threads::Cint
    )::CAPI.OrtStatusPtr
    CAPI.check_and_release(api, status)
end

function load_inference_single_threaded(path::AbstractString; execution_provider::Symbol=:cpu,
            envname::AbstractString="defaultenv", timer=ONNXRunTime.TIMER,
                       )::ONNXRunTime.InferenceSession
    api = GetApi(;execution_provider)
    env = CreateEnv(api, name=envname)
    if execution_provider === :cpu
        session_options = CreateSessionOptions(api)
        SetIntraOpNumThreads(api, session_options, 1)
    elseif execution_provider === :cuda
        if !(isdefined(@__MODULE__, :CUDA))
            @warn """
            The $(repr(execution_provider)) requires the CUDA.jl package to be available. Try adding `import CUDA` to your code.
            """
        end
        session_options = CreateSessionOptions(api)
        cuda_options = OrtCUDAProviderOptions()
        SessionOptionsAppendExecutionProvider_CUDA(api, session_options, cuda_options)
    else
        error("Unsupported execution_provider $execution_provider")
    end
    session = CreateSession(api, env, path, session_options)
    meminfo = CreateCpuMemoryInfo(api)
    allocator = CreateAllocator(api, session, meminfo)
    _input_names =ONNXRunTime.input_names(api, session, allocator)
    _output_names=ONNXRunTime.output_names(api, session, allocator)
    # TODO Is aliasing supported by ONNX? It will cause bugs, so lets forbid it.
    #@check allunique(_input_names)
    #@check allunique(_output_names)
    return ONNXRunTime.InferenceSession(api, execution_provider, session, meminfo, allocator,
        _input_names,
        _output_names,
        timer,
    )
end