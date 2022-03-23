global onnx_paths = IdDict()

function ReadONNX(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    global onnx_paths
    model_path = Banyan.getpath(loc_params["path"], comm)
    model = load_inference_single_threaded(model_path)
    onnx_paths[model] = model_path
    model
end

function ReadONNXFromDisk(
    src,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    onnx_path = getpath(loc_params["path"], comm) * "_onnx"
    model = load_inference_single_threaded(read(onnx_path, String))
    model
end

function WriteONNXToDisk(
    src,
    part,
    params,
    batch_idx::Int64,
    nbatches::Int64,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    global onnx_paths
    if get_partition_idx(batch_idx, nbatches, comm) == 1
        write(getpath(loc_params["path"], comm) * "_onnx", onnx_paths[part])
    end
    MPI.Barrier(comm)
end