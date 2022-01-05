function extract_dataset_path(remotepath)
    # Detect whether this is an HDF5 file
    hdf5_ending = if occursin(".h5", remotepath)
        ".h5"
    elseif occursin(".hdf5", remotepath)
        ".hdf5"
    else
        ""
    end
    isa_hdf5 = hdf5_ending != ""

    # Get the actual path by removing the dataset from the path
    remotepath, datasetpath = if hdf5_ending == ""
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

function RemoteHDF5Source(remotepath; shuffled=false, source_invalid = false, sample_invalid = false, invalidate_source = false, invalidate_sample = false)::Location
    RemoteSource(
        remotepath,
        shuffled=shuffled,
        source_invalid = source_invalid,
        sample_invalid,
        invalidate_source = invalidate_source,
        invalidate_sample = invalidate_sample
    ) do remotepath, remote_source, remote_sample, shuffled
        remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)

        if !isa_hdf5
            error("Expected HDF5 dataset for $remotepath")
        end

        # TODO: Cache stuff
        p = download_remote_path(remotepath)

        # TODO: Support more cases beyond just single files and all files in
        # given directory (e.g., wildcards)

        # TODO: Read cached sample if possible

        nbytes = 0
        totalnrows = 0

        # Handle single-file nd-arrays

        # TODO: Support HDF5 files that don't have .h5 in their filenmae
        # filename, datasetpath = split(p, hdf5_ending)
        # remotefilename, _ = split(remotepath, hdf5_ending)
        # filename *= hdf5_ending
        # remotefilename *= hdf5_ending
        # datasetpath = datasetpath[2:end] # Chop off the /

        # Load metadata for reading

        # TODO: Determine why sample size is so huge
        # TODO: Determine why location parameters are not getting populated

        # Open HDF5 file
        dset_sample = nothing
        datasize = nothing
        datandims = nothing
        dataeltype = nothing
        dataset_to_read_from_exists = false
        if isfile(p)
            with_downloaded_path_for_reading(p) do pp
                f = h5open(pp, "r")
                if haskey(f, datasetpath)
                    dataset_to_read_from_exists = true

                    dset = f[datasetpath]
                    ismapping = false
                    if HDF5.ismmappable(dset)
                        ismapping = true
                        dset = HDF5.readmmap(dset)
                        close(f)
                    end

                    # Collect metadata
                    nbytes += length(dset) * sizeof(eltype(dset))
                    datasize = size(dset)
                    datalength = first(datasize)
                    datandims = ndims(dset)
                    dataeltype = eltype(dset)

                    # TODO: Warn here if the data is too large
                    # TODO: Modify the alert that is given before sample collection starts
                    # TODO: Optimize utils_pfs.jl and generated code

                    memory_used_in_sampling = datalength == 0 ? 0 : (nbytes * Banyan.getsamplenrows(datalength) / datalength)
                    free_memory = Sys.free_memory()
                    if memory_used_in_sampling > cld(free_memory, 4)
                        @warn "Sample of $remotepath is too large (up to $(format_bytes(memory_used_in_sampling))/$(format_bytes(free_memory)) to be used). Try re-creating this job with a greater `sample_rate` than $(get_job().sample_rate)."
                        GC.gc()
                    end

                    if isnothing(remote_sample)
                        # Collect sample
                        totalnrows = datalength
                        remainingcolons = repeat([:], ndims(dset) - 1)
                        # Start of with an empty array. The dataset has to have at
                        # least one row so we read that in and then take no data.
                        # dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
                        # If the data is already shuffled or if we just want to
                        # take an exact sample, we don't need to randomly sample here.
                        if datalength > Banyan.get_max_exact_sample_length() && !shuffled
                            sampleindices = randsubseq(1:datalength, 1 / get_job().sample_rate)
                            # sample = dset[sampleindices, remainingcolons...]
                            if !isempty(sampleindices)
                                dset_sample = vcat([dset[sampleindex, remainingcolons...] for sampleindex in sampleindices]...)
                            end
                        end
                        
                        # Ensure that we have at least an empty initial array
                        if isnothing(dset_sample)
                            # NOTE: HDF5.jl does not support taking an empty slice
                            # so we have to read in the first row and then take a
                            # slice and this assumes that HDF5 datasets are always
                            # non-empty (which I think they always are).
                            dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
                        end

                        # Extend or chop sample as needed
                        samplelength = Banyan.getsamplenrows(datalength)
                        # TODO: Warn about the sample size being too large
                        if size(dset_sample, 1) < samplelength
                            dset_sample = vcat(
                                dset_sample,
                                dset[1:(samplelength-size(dset_sample, 1)), remainingcolons...],
                            )
                        else
                            dset_sample = dset[1:samplelength, remainingcolons...]
                        end
                    end

                    # Close HDF5 file
                    if !ismapping
                        close(f)
                    end
                end
            end
        end

        # If the sample is a PooledArray or CategoricalArray, convert it to a
        # simple array so we can correctly compute its memory usage.
        if !isnothing(dset_sample) && !(dset_sample isa Array)
            dset_sample = Banyan.convert_to_unpooled(dset_sample)
        end

        loc_for_reading, metadata_for_reading = if dataset_to_read_from_exists
            (
                "Remote",
                Dict(
                    "path" => remotepath,
                    "subpath" => datasetpath,
                    "size" => datasize,
                    "ndims" => datandims,
                    "eltype" => dataeltype,
                    "nbytes" => 0,
                    "format" => "hdf5"
                ),
            )
        else
            ("None", Dict{String,Any}())
        end

        # Get the remote sample
        if isnothing(remote_sample)
            remote_sample = if isnothing(loc_for_reading)
                Sample()
            elseif totalnrows <= Banyan.get_max_exact_sample_length()
                ExactSample(dset_sample, total_memory_usage = nbytes)
            else
                Sample(dset_sample, total_memory_usage = nbytes)
            end
        end

        # Construct location with metadata
        LocationSource(
            loc_for_reading,
            metadata_for_reading,
            nbytes,
            remote_sample,
        )
    end
end

function RemoteHDF5Destination(remotepath; invalidate_source = true, invalidate_sample = true)::Location
    RemoteDestination(p, invalidate_source = invalidate_source, invalidate_sample = invalidate_sample) do remotepath
        remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)

        if !isa_hdf5
            error("Expected HDF5 dataset for $remotepath")
        end

        # Load metadata for writing to HDF5 file
        loc_for_writing, metadata_for_writing =
            ("Remote", Dict("path" => remotepath, "subpath" => datasetpath, "nbytes" => 0, "format" => "hdf5"))

        LocationDestination(
            loc_for_writing,
            metadata_for_writing
        )
    end
end