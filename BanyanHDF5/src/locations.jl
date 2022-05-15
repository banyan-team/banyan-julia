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

# function get_remote_hdf5_source(
#     remotepath::String,
#     remote_source::Location,
#     remote_sample::Sample,
#     shuffled::Bool
# )::Location
#     remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)

#     if !isa_hdf5
#         error("Expected HDF5 dataset for $remotepath")
#     end

#     # TODO: Cache stuff
#     p = download_remote_path(remotepath)

#     # TODO: Support more cases beyond just single files and all files in
#     # given directory (e.g., wildcards)

#     # TODO: Read cached sample if possible

#     nbytes::Int64 = 0
#     totalnrows::Int64 = 0

#     # Handle single-file nd-arrays

#     # TODO: Support HDF5 files that don't have .h5 in their filenmae
#     # filename, datasetpath = split(p, hdf5_ending)
#     # remotefilename, _ = split(remotepath, hdf5_ending)
#     # filename *= hdf5_ending
#     # remotefilename *= hdf5_ending
#     # datasetpath = datasetpath[2:end] # Chop off the /

#     # Load metadata for reading

#     # TODO: Determine why sample size is so huge
#     # TODO: Determine why location parameters are not getting populated

#     # Open HDF5 file
#     dset_sample = nothing
#     datasize = nothing
#     datandims = nothing
#     dataeltype = nothing
#     dataset_to_read_from_exists = false
#     if isfile(p)
#         pp::String = get_downloaded_path(p)
#         f = h5open(pp, "r")
#         if haskey(f, datasetpath)
#             dataset_to_read_from_exists = true

#             dset = f[datasetpath]
#             ismapping = false
#             if HDF5.ismmappable(dset)
#                 ismapping = true
#                 dset = HDF5.readmmap(dset)
#                 close(f)
#             end

#             # Collect metadata
#             nbytes += length(dset) * sizeof(eltype(dset))
#             datasize = size(dset)
#             datalength = first(datasize)
#             datandims = ndims(dset)
#             dataeltype = eltype(dset)

#             # TODO: Warn here if the data is too large
#             # TODO: Modify the alert that is given before sample collection starts
#             # TODO: Optimize utils_pfs.jl and generated code

#             memory_used_in_sampling = datalength == 0 ? 0 : (nbytes * Banyan.getsamplenrows(datalength) / datalength)
#             free_memory = Sys.free_memory()
#             if memory_used_in_sampling > cld(free_memory, 4)
#                 @warn "Sample of $remotepath is too large (up to $(format_bytes(memory_used_in_sampling))/$(Banyan.format_bytes(free_memory)) to be used). Try re-starting this session with a greater `sample_rate` than $(get_session().sample_rate)."
#                 GC.gc()
#             end

#             if isnothing(remote_sample)
#                 # Collect sample
#                 totalnrows = datalength
#                 remainingcolons = repeat([:], ndims(dset) - 1)
#                 # Start of with an empty array. The dataset has to have at
#                 # least one row so we read that in and then take no data.
#                 # dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
#                 # If the data is already shuffled or if we just want to
#                 # take an exact sample, we don't need to randomly sample here.
#                 if datalength > Banyan.get_max_exact_sample_length() && !shuffled
#                     sampleindices = randsubseq(1:datalength, 1 / get_session().sample_rate)
#                     # sample = dset[sampleindices, remainingcolons...]
#                     if !isempty(sampleindices)
#                         dset_sample = vcat(map(sampleindex -> dset[sampleindex, remainingcolons...], sampleindices)...)
#                     end
#                 end
                
#                 # Ensure that we have at least an empty initial array
#                 if isnothing(dset_sample)
#                     # NOTE: HDF5.jl does not support taking an empty slice
#                     # so we have to read in the first row and then take a
#                     # slice and this assumes that HDF5 datasets are always
#                     # non-empty (which I think they always are).
#                     dset_sample = dset[1:1, remainingcolons...][1:0, remainingcolons...]
#                 end

#                 # Extend or chop sample as needed
#                 samplelength = Banyan.getsamplenrows(datalength)
#                 # TODO: Warn about the sample size being too large
#                 if size(dset_sample, 1) < samplelength
#                     dset_sample = vcat(
#                         dset_sample,
#                         dset[1:(samplelength-size(dset_sample, 1)), remainingcolons...],
#                     )
#                 else
#                     dset_sample = dset[1:samplelength, remainingcolons...]
#                 end
#             end

#             # Close HDF5 file
#             if !ismapping
#                 close(f)
#             end
#         end
#         destroy_downloaded_path(pp)
#     end

#     # If the sample is a PooledArray or CategoricalArray, convert it to a
#     # simple array so we can correctly compute its memory usage.
#     if !isnothing(dset_sample) && !(dset_sample isa Base.Array)
#         dset_sample = Base.convert(Base.Array, dset_sample)
#     end

#     loc_for_reading, metadata_for_reading = if dataset_to_read_from_exists
#         (
#             "Remote",
#             Dict{String,Any}(
#                 "path" => remotepath,
#                 "subpath" => datasetpath,
#                 "size" => datasize,
#                 "ndims" => datandims,
#                 "eltype" => dataeltype,
#                 "nbytes" => 0,
#                 "format" => "hdf5"
#             ),
#         )
#     else
#         ("None", Dict{String,Any}())
#     end

#     # Get the remote sample
#     if isnothing(remote_sample)
#         remote_sample::Sample = if isnothing(loc_for_reading)
#             Sample()
#         elseif totalnrows <= Banyan.get_max_exact_sample_length()
#             ExactSample(dset_sample, nbytes)
#         else
#             Sample(dset_sample, nbytes)
#         end
#     end

#     # Construct location with metadata
#     LocationSource(
#         loc_for_reading,
#         metadata_for_reading,
#         nbytes,
#         remote_sample,
#     )
# end

function _remote_hdf5_source(path_and_subpath, shuffled, metadata_invalid, sample_invalid, invalidate_metadata, invalidate_sample, max_exact_sample_length)
    # Get session information
    session_sample_rate = get_session().sample_rate
    worker_idx, nworkers = get_worker_idx(), get_nworkers()
    is_main = worker_idx == 1
    max_exact_sample_length = max_exact_sample_length >= 0 ? max_exact_sample_length : get_max_exact_sample_length()

    # Get current location
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location(path_and_subpath, metadata_invalid, sample_invalid)
    if !curr_parameters_invalid && !curr_sample_invalid
        return curr_location
    end

    # Download the path
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(path_and_subpath)
    isa_hdf5 || error("Expected HDF5 file for $remotepath")
    p = getpath(remotepath)
    HDF5.ishdf5(p) || "Expected HDF5 file at $remotepath"

    # Open HDF5 file
    dataset_to_read_from_exists = false
    f = h5open(p, "r")
    haskey(f, datasetpath) || "Expected HDF5 dataset named \"$datasetpath\" in $remotepath"
    dataset_to_read_from_exists = true

    # Open the dataset
    dset = f[datasetpath]
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
        rand_indices = sample_from_range(rand_indices_range, session_sample_rate)
        exact_sample_needed = datalength < max_exact_sample_length
        remaining_colons = Base.fill(Colon(), datandims-1)
        println("In HDF5 sample collection with rand_indices_range=$rand_indices_range, rand_indices=$rand_indices, worker_idx=$worker_idx, max_exact_sample_length=$max_exact_sample_length, datalength=$datalength, exact_sample_needed=$exact_sample_needed, shuffled=$shuffled")
        dset_sample_value = if !exact_sample_needed
            samples_on_workers = gather_across(
                if shuffled || isempty(rand_indices)
                    range_for_this_worker = rand_indices_range.start:(rand_indices_range.start+length(rand_indices)-1)
                    dset[range_for_this_worker, remaining_colons...]
                else
                    vcat(
                        (
                            dset[rand_index, remaining_colons...]
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
                Sample(dset_sample_value, nbytes)
            end
        else
            NOTHING_SAMPLE
        end
    else
        curr_location.sample
    end

    # Close HDF5 file
    close(f)

    if is_main
        location_res = LocationSource(
            "Remote",
            Dict{String,Any}(
                "path_and_subpath" => path_and_subpath,
                "path" => remotepath,
                "subpath" => datasetpath,
                "size" => datasize,
                "ndims" => datandims,
                "eltype" => dataeltype,
                "nbytes" => nbytes,
                "format" => "hdf5"
            ),
            nbytes,
            dset_sample,
        )
        @show location_res
        @show total_memory_usage(location_res)
        @show total_memory_usage(location_res.sample)
        @show total_memory_usage(location_res.sample.value)
        cache_location(remotepath, location_res, invalidate_sample, invalidate_metadata)
        location_res
    else
        INVALID_LOCATION
    end
end

function RemoteHDF5Source(remotepath; shuffled=false, metadata_invalid = false, sample_invalid = false, invalidate_metadata = false, invalidate_sample = false, max_exact_sample_length = -1)::Location
    offloaded(
        _remote_hdf5_source,
        remotepath,
        shuffled,
        metadata_invalid,
        sample_invalid,
        invalidate_metadata,
        invalidate_sample,
        max_exact_sample_length;
        distributed=true
    )
end

function RemoteHDF5Destination(remotepath)::Location
    remotepath, datasetpath, isa_hdf5 = extract_dataset_path(remotepath)
    isa_hdf5 || error("Expected HDF5 dataset for $remotepath")
    LocationDestination(
        "Remote",
        Dict{String,Any}(
            "path" => remotepath,
            "subpath" => datasetpath,
            "nbytes" => 0,
            "format" => "hdf5"
        )
    )
end
