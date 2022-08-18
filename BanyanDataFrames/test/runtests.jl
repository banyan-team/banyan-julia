using Banyan
using BanyanArrays
using FilePathsBase, AWSS3, DataFrames, Downloads, CSV, Parquet, Arrow
using BanyanDataFrames
using ReTest
using Random
using Statistics

include("utils_sessions.jl")

include("utils_data.jl")#
include("groupby_filter_indexing.jl")
# include("test_jobs.jl")

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
        "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
    else
        error("Unsupported file extension: $file_extension")
    end

    # Return the path to be passed into a read_* function
    if remote_kind == "Internet"
        url * (file_extension_is_hdf5 ? "/DS1" : "")
    elseif remote_kind == "Disk"
        # Get names and paths
        testing_dataset_local_name =
            (file_extension_is_hdf5 ? "fillval" : "iris") * ".$file_extension"
        testing_dataset_local_path =
            joinpath(homedir(), ".banyan", "testing_datasets", testing_dataset_local_name)

        # Download if not already download
        if !isfile(testing_dataset_local_path)
            # Download to local ~/.banyan/testing_datasets
            mkpath(joinpath(homedir(), ".banyan", "testing_datasets"))
            Downloads.download(url, testing_dataset_local_path)

            # Convert file if needed
            if file_extension == "parquet" || file_extension == "arrow"
                df = CSV.read(testing_dataset_local_path, DataFrames.DataFrame)
                df.species = string.(df.species)
            end
            if file_extension == "parquet"
                Parquet.write_parquet(testing_dataset_local_path, df)
            elseif file_extension == "arrow"
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
            config = Banyan.global_aws_config(),
        )

        # Create the file if not already created
        if !ispath(testing_dataset_s3_path)
            # Download if not already download
            if !isfile(testing_dataset_local_path)
                # Download to local ~/.banyan/testing_datasets
                mkpath(joinpath(homedir(), ".banyan", "testing_datasets"))
                Downloads.download(url, testing_dataset_local_path)

                # Convert file if needed
                df = CSV.read(testing_dataset_local_path, DataFrames.DataFrame)
                df.species = string.(df.species)
                if file_extension == "parquet"
                    Parquet.write_parquet(testing_dataset_local_path, df)
                    # cp(testing_dataset_local_path, "$(homedir())/iris_in_a_file.parquet", force=true)
                elseif file_extension == "arrow"
                    Arrow.write(testing_dataset_local_path, df)
                end
            end

            # Upload to S3
            if single_file
                cp(Path(testing_dataset_local_path), testing_dataset_s3_path)
                println("Copying testing_dataset_local_path=$testing_dataset_local_path to testing_dataset_s3_path=$testing_dataset_s3_path for file_extension=$file_extension")
            else
                for i = 0:19
                    dst = joinpath(testing_dataset_s3_path, "part_$i.$file_extension")
                    if !isfile(dst)
                        cp(
                            Path(testing_dataset_local_path),
                            dst,
                        )
                    end
                end
            end
        end

        string(testing_dataset_s3_path) * (file_extension_is_hdf5 ? "/DS1" : "")
    else
        error("Unsupported kind of remote: $remote_kind")
    end
end

# TODO: Break up these use_* functions to be parametrized on the data format
# and only load in the files for that format. Then, testsets in
# groupby_filter_indexing.jl can iterate over all file types

function use_basic_data()
    cleanup_tests()
    # NOTE: There might be an issue here where because of S3's eventual
    # consistency property, this causes failures in writing new files
    # that are being deleted where the delete happens after the new write.
    setup_basic_tests()
end

function use_stress_data()
    cleanup_tests()
    setup_stress_tests()
end

function use_empty_data()
    cleanup_tests()
    setup_basic_tests()
    setup_empty_tests()
end

include("sample_computation.jl")
include("sample_collection.jl")
include("latency.jl")
include("groupby_filter_indexing.jl")

# Clear caches to ensure that caching behavior is deterministic
# Actually, don't clear this until we optimize sample/source collection.
# Until then, we have the sample collection tests for this.
# invalidate_all_sources()
# invalidate_all_samples()

try
    runtests(Regex.(ARGS)...)
finally
    # End sessions to clean up
    end_all_sessions_for_testing()
end