@testset "Simple usage of HDF5 in $src with $scheduling_config" for scheduling_config in [
    "default scheduling",
    "parallelism encouraged",
    "parallelism and batches encouraged",
],
src in ["Internet", "S3"]
    use_session_for_testing(scheduling_config_name = scheduling_config, sample_rate = 20) do
        use_data()
        configure_sampling(max_num_bytes_exact=0)

        for _ in 1:2
            src_is_s3 = src == "S3"
            x = if src_is_s3
                read_hdf5(joinpath("s3://", get_cluster_s3_bucket_name(), "fillval.h5/DS1"), metadata_invalid=true, sample_invalid=true)
            else
                read_hdf5(joinpath("https://github.com/banyan-team/banyan-julia/raw/v0.1.1/BanyanArrays/test/res", "fillval.h5/DS1"), metadata_invalid=true, sample_invalid=true)
            end

            # Test basic case of reading from remote file
            x_length_collect = length(x)
            @test x_length_collect == (src_is_s3 ? 240 : 600000)
            x_size_collect = size(x)
            @test x_size_collect == (src_is_s3 ? (20, 12) : (1000, 600))
            x_sum_collect = compute(sum(x))
            @test x_sum_collect == (src_is_s3 ? 12840 : 32100000)
            x_sum_collect = compute(sum(x))
            @test x_sum_collect == (src_is_s3 ? 12840 : 32100000)
        end

        configure_sampling(default=true)
    end
end

# TODO: Add tests here modeled after BDF.jl

@testset "Reading $(shuffled ? "shuffled " : "")$src data and sampling it with $scheduling_config and a maximum of $max_num_bytes bytes for exact sample" for
    scheduling_config in [
        "default scheduling",
        "parallelism encouraged",
        "parallelism and batches encouraged",
    ],
    src in ["Internet", "S3"],
    max_num_bytes in [0, 100_000_000_000],
    shuffled in [true, false]
    
    get_organization_id()
    use_session_for_testing(scheduling_config_name = scheduling_config) do
        use_data()
        configure_sampling(max_num_bytes_exact=max_num_bytes, always_shuffled=shuffled, for_all_locations=true, default=true)
        exact_sample = max_num_bytes > 0

        invalidate_all_locations()

        p = if src == "S3"
            joinpath("s3://", get_cluster_s3_bucket_name(), "fillval.h5/DS1")
        else
            joinpath("https://github.com/banyan-team/banyan-julia/raw/v0.1.1/BanyanArrays/test/res", "fillval.h5/DS1")
        end

        df = read_hdf5(p; metadata_invalid=true, invalidate_samples=true)
        sample(df)

        configure_sampling(p; sample_rate=5)
        read_hdf5(p)
        @test get_sample_rate(p) == 5
        @test has_metadata(p)
        @test has_sample(p)
        invalidate_metadata(p)
        @test !has_metadata(p)
        @test has_sample(p)
        invalidate_location(p)
        @test !has_metadata(p)
        @test !has_sample(p)

        df2 = read_hdf5(p)
        sample(df2)
        df2 = read_hdf5(p; samples_invalid=true)
        sample(df2)
        @test get_sample_rate(p) == 5
        configure_sampling(sample_rate=7, for_all_locations=true)
        @test get_sample_rate(p) == 5
        df2 = read_hdf5(p; metadata_invalid=true)
        sample(df2)
        @test get_sample_rate(p) == 5
        @test get_sample_rate() == 7
        configure_sampling(sample_rate=7, for_all_locations=true)
        @test get_sample_rate(p) == 5
        configure_sampling(sample_rate=7, force_new_sample_rate=true, for_all_locations=true)
        @test get_sample_rate(p) == 7
        @test get_sample_rate() == 7
        df2 = read_hdf5(p)
        @test get_sample_rate(p) == 7
        @test get_sample_rate() == 7
        df2 = read_hdf5(p; location_invalid=true)
        sample(df2)
        @test has_metadata(p)
        @test has_sample(p)
        configure_sampling(p; always_exact=true)
        sample(df2)
    end
end

@testset "Reading/writing 2D arrays with HDF5 in $src with $scheduling_config" for scheduling_config in
                                                                                     [
        "default scheduling",
        "parallelism encouraged",
        "parallelism and batches encouraged",
    ],
    src in ["Internet", "S3"]

    use_session_for_testing(scheduling_config_name = scheduling_config, sample_rate = 20) do
        use_data(src)
        configure_sampling(max_num_bytes_exact=0)

        # Determine where to read from

        # TODO: Make this more general by creating S3 bucket and uploading
        # file from test/res for testing
        # TODO: Use version of `pt_lib_info.json` with replication actually removed
        src_is_internet = src == "Internet"
        path = if src_is_internet
            # TODO: Test Internet
            # "https://github.com/banyan-team/banyan-julia/blob/v0.1.1/BanyanArrays/test/res/fillval.h5?raw=true",
            "https://github.com/banyan-team/banyan-julia/raw/v0.1.1/BanyanArrays/test/res/fillval.h5"
        else
            # The file is produced in S3 using:
            # wget https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5
            # AWS_DEFAULT_PROFILE=banyan-testing aws s3 cp \
            # h5ex_d_fillval.h5 \
            # s3://banyan-cluster-data-pumpkincluster0-3e15290827c0c584/fillval.h5
            # Then, repeat the dataset by (100, 100)
            joinpath("s3://", get_cluster_s3_bucket_name(), "fillval.h5")
        end

        # Perform computation

        x = read_hdf5(joinpath(path, "DS1"), metadata_invalid=true, sample_invalid=true)
        # @show Base.collect(length(x))
        x = map(e -> e * 10, x)

        steps = if startswith(path, "s3://")
            # We don't currently support writing
            1:1
        else
            # It's fine to ignore steps 2 and 3 b/c there's no reason I
            # can think of for why reading from Internet and then writing
            # to S3 would not work just because we read from the Internet
            1:1
        end
        for step in steps
            if step == 2
                # TODO: Maybe delete these datasets/files before writing
                # to them to ensure that we don't have issues where we need
                # to fsync the data into S3 before being able to read data.
                # Test writing to and reading from dataset in group in
                # group in same file
                copied_path = joinpath(path, "copies", "DS2")
                write_hdf5(x, copied_path)
                x = read_hdf5(copied_path)
            elseif step == 3
                # Test writing to different file and then reading from it
                copied_path = path[1:end-3] * "_copy.h5/DS1"
                write_hdf5(x, copied_path)
                x = read_hdf5(copied_path)
            end

            # Test basic case of reading from remote file
            x_length_collect = length(x)
            @test x_length_collect == (src_is_internet ? 600000 : 240)
            x_size_collect = size(x)
            @test x_size_collect == (src_is_internet ? (1000, 600) : (20, 12))
            x_sum_collect = compute(sum(x)) # here?
            @test x_sum_collect == (src_is_internet ? 321000000 : 128400) # incorrect
            x_sum_collect = compute(sum(x))
            @test x_sum_collect == (src_is_internet ? 321000000 : 128400) # incorrect
            x_minimum_collect = compute(minimum(x))
            @test x_minimum_collect == -60
            x_maximum_collect = compute(maximum(x))
            @test x_maximum_collect == 990
            x_length_collect = length(x)
            @test x_length_collect == (src_is_internet ? 600000 : 240)
            x_size_collect = size(x)
            @test x_size_collect == (src_is_internet ? (1000, 600) : (20, 12))
        end


        # TODO: Re-enable this test once we ensure that we can write out small
        # enough datasets without unnecessary batching. This test involves
        # multiple reductions on the same dataset but the scheduler doesn't
        # think the dataset can stay in memory between evaluations so it tries
        # to persist it to disk. And this fails because we don't support
        # writing HDF5 datasets with strings yet.
        # run_with_job("Reading/writing string arrays with HDF5") do job
        #     for path in [
        #         # "https://github.com/banyan-team/banyan-julia/blob/v0.1.1/BanyanArrays/test/res/vlstring.h5?raw=true",
        #         "https://github.com/banyan-team/banyan-julia/raw/v0.1.1/BanyanArrays/test/res/vlstring.h5",
        #         "s3://banyan-cluster-data-pumpkincluster0-3e15290827c0c584/vlstring.h5",
        #     ]
        #         x = read_hdf5(joinpath(path, "DS1"))
        #         x = map(identity, x)

        #         # TODO: Use all 3 steps so that we can test out writing once
        #         # we get writing strings to work
        #         steps = 1:1
        #         # steps = if startswith(path, "s3://")
        #         #     1:3
        #         # else
        #         #     1:1
        #         # end
        #         for step in steps
        #             if step == 2
        #                 # Test writing to and reading from dataset in group in
        #                 # group in same file
        #                 copied_path = joinpath(path, "copies", "DS2")
        #                 write_hdf5(x, copied_path)
        #                 x = read_hdf5(copied_path)
        #             elseif step == 3
        #                 # Test writing to different file and then reading from it
        #                 copied_path = path[1:end-3] * "_copy.h5/DS1"
        #                 write_hdf5(x, copied_path)
        #                 x = read_hdf5(copied_path)
        #             end

        #             # Test basic case of reading from remote file
        #             x_length_collect = length(x)
        #             @test x_length_collect == 400
        #             x_size_collect = size(x)
        #             @test x_size_collect == (400,)
        #             x_minimum_collect = Base.collect(minimum(x))
        #             @test x_minimum_collect == "Parting"
        #             x_maximum_collect = Base.collect(maximum(x))
        #             @test x_maximum_collect == "sweet"
        #             x_length_collect = length(x)
        #             @test x_length_collect == 400
        #             x_size_collect = size(x)
        #             @test x_size_collect == (400,)
        #         end
        #     end
        # end

        configure_sampling(default=true)
    end
end
