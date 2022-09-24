function extract_dataset_path(remotepath::String)::Tuple{String,String,Bool}
    # Detect whether this is an HDF5 file
    hdf5_ending::String = if occursin(".h5", remotepath)
        ".h5"
    elseif occursin(".hdf5", remotepath)
        ".hdf5"
    else
        ""
    end
    isa_hdf5 = hdf5_ending != ""

    # Get the actual path by removing the dataset from the path
    remotepath::String, datasetpath::String = if hdf5_ending == ""
        remotepath, nothing
    else
        remotepath, datasetpath = split(remotepath, hdf5_ending)
        remotepath *= hdf5_ending # Add back the file extension
        datasetpath = datasetpath[2:end] # Chop off the /
        # NOTE: It's critical that we convert `datasetpath` from a SubString
        # to a String because then the `haspath` on an `HDF5.File` will fail
        remotepath, String(datasetpath)
    end

    remotepath, datasetpath, isa_hdf5
end

HDF5_getindex_retry = retry(HDF5.getindex; delays=Base.ExponentialBackOff(; n=5))

function _remote_hdf5_source(lp::LocationPath, loc::Location)
    sc = get_sampling_config(lp)
    path_and_subpath = lp.path
    shuffled = sc.assume_shuffled
    curr_metadata_invalid, curr_sample_invalid = loc.metadata_invalid, loc.sample_invalid

    # Get session information
    sample_rate = sc.rate
    worker_idx, nworkers = get_worker_idx(), get_nworkers()
    is_main = worker_idx == 1

    # Get current location
    if !curr_metadata_invalid && !curr_sample_invalid
        return loc
    end

    # Download the path
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(path_and_subpath)
    isa_hdf5 || error("Expected HDF5 file for $remotepath")
    p = getpath(remotepath)
    HDF5.ishdf5(p) || "Expected HDF5 file at $remotepath"

    if is_main
        f = h5open(p, "r")
        close(f)
    end
    sync_across()

    # Open HDF5 file
    dataset_to_read_from_exists = false
    f = h5open(p, "r")
    haskey(f, datasetpath) || error("Expected HDF5 dataset named \"$datasetpath\" in $remotepath")
    dataset_to_read_from_exists = true

    # Open the dataset
    dset = try
        HDF5_getindex_retry(f, datasetpath)
    catch
        close(f)
        f = h5open(p, "r")
        haskey(f, datasetpath) || error("Expected HDF5 dataset named \"$datasetpath\" in $remotepath")
        getindex(f, datasetpath)
    end
    # TODO: Support mmap
    # ismapping = false
    # if HDF5.ismmappable(dset)
    #     ismapping = true
    #     dset = HDF5.readmmap(dset)
    #     close(f)
    # end

    # Collect metadata
    nbytes = length(dset) * sizeof(eltype(dset))
    datasize = size(dset)
    datalength = datasize[1]
    datandims = ndims(dset)
    dataeltype = eltype(dset)

    # Collect sample
    dset_sample = if curr_sample_invalid
        # Read in the sample on each worker and
        # aggregate and concatenate it on the main worker
        rand_indices_range = split_len(datalength, worker_idx, nworkers)
        rand_indices = sample_from_range(rand_indices_range, sample_rate)
        exact_sample_needed = nbytes < sc.max_num_bytes_exact || sc.always_exact
        remaining_colons = Base.fill(Colon(), datandims-1)
        dset_sample_value = if !exact_sample_needed
            samples_on_workers = gather_across(
                if shuffled || isempty(rand_indices)
                    range_for_this_worker = rand_indices_range.start:(rand_indices_range.start+length(rand_indices)-1)
                    dset[range_for_this_worker, remaining_colons...]
                else
                    vcat(
                        (
                            let dset_read = dset[rand_index, remaining_colons...]
                                reshape(dset_read, (1, size(dset_read)...))
                            end
                            for rand_index in rand_indices
                        )...
                    )
                end
            )
            vcat(samples_on_workers...)
        else
            dset[:, remaining_colons...]
        end

        # Return a `Sample` on the main worker
        if is_main
            # If the sample is a PooledArray or CategoricalArray, convert it to a
            # simple array so we can correctly compute its memory usage.
            if !(dset_sample_value isa Base.Array)
                dset_sample_value = Base.convert(Base.Array, dset_sample)
            end
            if exact_sample_needed
                ExactSample(dset_sample_value, nbytes)
            else
                Sample(dset_sample_value, nbytes, sample_rate)
            end
        else
            deepcopy(NOTHING_SAMPLE)
        end
    else
        deepcopy(NOTHING_SAMPLE)
    end

    # Close HDF5 file
    close(f)

    if is_main
        # Construct parameters for Location
        src_params = if curr_metadata_invalid
            Dict{String,Any}(
                "name" => "Remote",
                "path_and_subpath" => path_and_subpath,
                "path" => remotepath,
                "subpath" => datasetpath,
                "eltype" => Banyan.type_to_str(dataeltype),
                "size" => Banyan.size_to_str(datasize),
                "sample_memory_usage" => string(nbytes),
                "format" => "hdf5"
            )
        else
            loc.src_parameters
        end

        # Get paths to store metadata and sample in
        metadata_path = "s3/$(banyan_metadata_bucket_name())/$(get_metadata_path(lp))"
        sample_dir = "s3/$(banyan_samples_bucket_name())/$(get_sample_path_prefix(lp))"
        mkpath(sample_dir)
        sample_path = "$sample_dir/$sample_rate"

        # Store metadata and sample in S3
        if curr_metadata_invalid
            Arrow.write(metadata_path, Arrow.Table(); metadata=src_params)
        end
        if curr_sample_invalid
            serialize(sample_path, dset_sample)
        end

        # Return Location to client side
        LocationSource("Remote", src_params, nbytes, dset_sample)
    else
        INVALID_LOCATION
    end
end

RemoteHDF5Source(remotepath)::Location =
    RemoteSource(
        LocationPath(remotepath),
        _remote_hdf5_source,
        deserialize,
        identity,
        serialize
    )

function RemoteHDF5Destination(remotepath)::Location
    path_and_subpath = remotepath
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)
    isa_hdf5 || error("Expected HDF5 dataset for $remotepath")
    LocationDestination(
        "Remote",
        Dict{String,Any}(
            "path" => remotepath,
            "subpath" => datasetpath,
            "path_and_subpath" => path_and_subpath,
            "format" => "hdf5",
        )
    )
end
