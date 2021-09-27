using ReTest
using DataFrames, CSV, Parquet, Arrow

global jobs_for_testing = Dict()

function use_job_for_testing(;
    sample_rate = 2,
    max_exact_sample_length = 50,
    with_s3fs = nothing,
    scheduling_config_name = "default scheduling",
)
    haskey(ENV, "BANYAN_CLUSTER_NAME") || error(
        "Please specify the Banyan cluster to use for testing with the BANYAN_CLUSTER_NAME environment variable",
    )

    # This will be a more complex hash if there are more possible ways of
    # configuring a job for testing
    job_config_hash = sample_rate

    # Set the job and create a new one if needed
    global jobs_for_testing
    set_job_id(
        if haskey(jobs_for_testing, job_config_hash)
            jobs_for_testing[job_config_hash]
        else
            create_job(
                cluster_name = ENV["BANYAN_CLUSTER_NAME"],
                nworkers = 2,
                banyanfile_path = "file://res/BanyanfileDebug.json",
                sample_rate = sample_rate,
                return_logs = true,
            )
        end,
    )

    # Set the maximum exact sample length
    ENV["MAX_EXACT_SAMPLE_LENGTH"] = string(max_exact_sample_length)

    # Force usage of S3FS if so desired
    if !isnothing(with_s3fs)
        ENV["BANYAN_USE_S3FS"] = with_s3fs ? "1" : "0"
    end

    configure_scheduling(name = scheduling_config_name)
end

function use_data(file_extension, remote_kind, single_file)
    # TODO: Handle case where file_extension == "hdf5" and single_file=false
    file_extension_is_hdf5 =
        file_extension == "h5" || file_extension == "hdf5" || file_extension == "hdf"
    file_extension_is_table =
        file_extension == "csv" || file_extension == "parquet" || file_extension == "arrow"

    if file_extension_is_hdf5 && !single_file
        error("HDF5 datasets must be a single file")
    end

    # Determine URL
    url = if file_extension_is_hdf5
        "https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5"
    elseif file_extension_is_table
        "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
    else
        error("Unsupported file extension: $file_extension")
    end

    # Return the path to be passed into a read_* function
    if remote_kind == "Internet"
        url * file_extension_is_hdf5 ? "/DS1" : ""
    elseif remote_kind == "Disk"
        # Get names and paths
        testing_dataset_local_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") * ".$file_extension"
        testing_dataset_local_path =
            joinpath(homedir(), ".banyan", "testing_datasets", testing_dataset_local_name)
            
        # Download if not already download
        if !isfile(testing_dataset_local_path)
            # Download to local ~/.banyan/testing_datasets
            download(url, testing_dataset_local_path)

            # Convert file if needed
            if file_extension == "parquet"
                df = CSV.read(testing_dataset_local_path, DataFrame)
                write_parquet(testing_dataset_local_path, df)
            elseif file_extension == "arrow"
                df = CSV.read(testing_dataset_local_path, DataFrame)
                Arrow.write(testing_dataset_local_path, df)
            end
        end

        testing_dataset_local_path
    elseif remote_kind == "S3"
        # Get names and paths
        testing_dataset_local_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") * ".$file_extension"
        testing_dataset_local_path =
            joinpath(homedir(), ".banyan", "testing_datasets", testing_dataset_local_name)
        testing_dataset_s3_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") *
            "_in_a_" *
            (single_file ? "file" : "dir") *
            ".$file_extension"
        testing_dataset_s3_path = S3Path(
            "s3://$(get_cluster_s3_bucket_name())/$testing_dataset_s3_name",
            config = get_aws_config(),
        )

        # Create the file if not already created
        if !ispath(testing_dataset_s3_path)
            # Download if not already download
            if !isfile(testing_dataset_local_path)
                # Download to local ~/.banyan/testing_datasets
                download(url, testing_dataset_local_path)

                # Convert file if needed
                if file_extension == "parquet"
                    df = CSV.read(testing_dataset_local_path, DataFrame)
                    write_parquet(testing_dataset_local_path, df)
                elseif file_extension == "arrow"
                    df = CSV.read(testing_dataset_local_path, DataFrame)
                    Arrow.write(testing_dataset_local_path, df)
                end
            end

            # Upload to S3
            if single_file
                cp(testing_dataset_local_path, testing_dataset_s3_path)
            else
                for i = 0:9
                    cp(
                        testing_dataset_local_path,
                        joinpath(testing_dataset_s3_path, "part_$i.$file_extension"),
                    )
                end
            end
        end

        string(testing_dataset_s3_path) * file_extension_is_hdf5 ? "/DS1" : ""
    else
        error("Unsupported kind of remote: $remote_kind")
    end
end

# This function is sort of a convenience for testing - it allows us to use
# a single string name to vaguely describe what data we want and to then
# specify if we want it on the Internet or on S3. Motivated because not
# every permutation of the values of the parameters for the other `use_data`
# are valid (e.g., HDF5 data cannot _not_ be a single file).
use_data(name, remote_kind) =
    use_data(last(split(name, ".")), remote_kind, contains(name, "file"))

include("sample_collection.jl")
include("sample_computation.jl")

try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up
    for job_id in values(jobs_for_testing)
        destroy_job(job_id)
    end
end