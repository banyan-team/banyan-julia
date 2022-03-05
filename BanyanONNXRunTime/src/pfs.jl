global onnx_paths = IdDict()

function ReadONNX(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
    comm::MPI.Comm,
    loc_name,
    loc_params,
)
    println("At start of ReadONNX")
    global onnx_paths
    model_path = Banyan.getpath(loc_params["path"], comm)
    println("Using model at path $model_path")
    model = load_inference_single_threaded(model_path)
    onnx_paths[model] = model_path
    println("At end of ReadONNX")
    model
end

function ReadONNXFromDisk(
    src,
    params,
    batch_idx::Integer,
    nbatches::Integer,
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
    batch_idx::Integer,
    nbatches::Integer,
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