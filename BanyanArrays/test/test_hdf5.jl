@testset "HDF5" begin
    run_with_job("Reading/writing 2D arrays with HDF5") do job
        # TODO: Make this more general by creating S3 bucket and uploading
        # file from test/res for testing
        # TODO: Use version of `pt_lib_info.json` with replication actually removed
        for path in [
            "https://github.com/banyan-team/banyan-julia/blob/v0.1.1/BanyanArrays/test/res/fillval.h5?raw=true",
            # The file is produced in S3 using:
            # AWS_DEFAULT_PROFILE=banyan-testing aws s3 \
            # cp https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5 \
            # banyan-cluster-data-pumpkincluster0-3e15290827c0c584/h5ex_d_fillval.h5
            "s3://banyan-cluster-data-pumpkincluster0-3e15290827c0c584/fillval.h5",
        ]
            x = read_hdf5(joinpath(path, "DS1"))
            x = map(e -> e * 10, x)

            steps = if startswith(path, "s3://")
                1:3
            else
                # It's fine to ignore steps 2 and 3 b/c there's no reason I
                # can think of for why reading from Internet and then writing
                # to S3 would not work just because we read from the Internet
                1:1
            end
            for step in steps
                if step == 2
                    # Test writing to and reading from dataset in group in
                    # group in same file
                    copied_path = joinpath(path, "copies", "DS2")
                    write_hdf5(x, copied_path)
                    x = read_hdf5(copied_path)
                elseif step == 3
                    # Test writing to different file and then reading from it
                    copied_path = path[1:end-3] * "_copy.h5"
                    write_hdf5(x, copied_path)
                    x = read_hdf5(copied_path)
                end

                # Test basic case of reading from remote file
                x_length_collect = collect(length(x))
                @test x_length_collect == 600000
                x_size_collect = collect(size(x))
                @test x_size_collect == (1000, 600)
                x_sum_collect = collect(sum(x))
                @test collect(sum(x)) == 321000000
                x_minimum_collect = collect(minimum(x))
                @test x_minimum_collect == -60
                x_maximum_collect = collect(maximum(x))
                @test x_maximum_collect == 990
                x_length_collect = collect(length(x))
                @test x_length_collect == 600000
                x_size_collect = collect(size(x))
                @test x_size_collect == (1000, 600)
            end
        end

        run_with_job("Reading/writing string arrays with HDF5") do job
            for path in [
                "https://github.com/banyan-team/banyan-julia/blob/v0.1.1/BanyanArrays/test/res/vlstring.h5?raw=true",
                "s3://banyan-cluster-data-pumpkincluster0-3e15290827c0c584/vlstring.h5",
            ]
                x = read_hdf5(joinpath(path, "DS1"))
                x = map(identity, x)

                steps = if startswith(path, "s3://")
                    1:3
                else
                    1:1
                end
                for step in steps
                    if step == 2
                        # Test writing to and reading from dataset in group in
                        # group in same file
                        copied_path = joinpath(path, "copies", "DS2")
                        write_hdf5(x, copied_path)
                        x = read_hdf5(copied_path)
                    elseif step == 3
                        # Test writing to different file and then reading from it
                        copied_path = path[1:end-3] * "_copy.h5"
                        write_hdf5(x, copied_path)
                        x = read_hdf5(copied_path)
                    end

                    # Test basic case of reading from remote file
                    x_length_collect = collect(length(x))
                    @test x_length_collect == 400
                    x_size_collect = collect(size(x))
                    @test collect(size(x)) == (400,)
                    x_minimum_collect = collect(minimum(x))
                    @test collect(minimum(x)) == "Parting"
                    x_maximum_collect = collect(maximum(x))
                    @test collect(maximum(x)) == "sweet"
                    x_length_collect = collect(length(x))
                    @test collect(length(x)) == 400
                    x_size_collect = collect(size(x))
                    @test collect(size(x)) == (400,)
                end
            end
        end
    end
end
