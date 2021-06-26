@testset "Loading BanyanArrays from HDF5 datasets" begin
    run_with_job("Load from HDF5 on the Internet") do job
        # TODO: Make this more general by creating S3 bucket and uploading
        # file from test/res for testing
        # TODO: Use version of `pt_lib_info.json` with replication actually removed
        for path in [
            "fillval.h5/DS1",
            # The file is produced in S3 using:
            # AWS_DEFAULT_PROFILE=banyan-testing aws s3 \
            # cp https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5 \
            # banyan-cluster-data-pumpkincluster0-3e15290827c0c584/h5ex_d_fillval.h5
            "fillval.h5/DS1"
        ]
            x = read_hdf5("path")
            x = map(e -> e * 10, x)

            @test collect(length(x)) == 60
            @test collect(size(x)) == (10,6)
            @test collect(sum(x)) == 32100
            @test collect(minimum(x)) == 0
            @test collect(maximum(x)) == 990
            @test collect(length(x)) == 60
            @test collect(size(x)) == (10,6)
        end

        # TODO: Add test case for vlen, vlstring
        # TODO: Duplicate 100 times size for testing
        # TODO: Add test case for S3
        # TODO: Add test case for writing dataset back to a subgroup and then reading it
    end

    # run_with_job("Multiple evaluations apart") do job
    #     x = BanyanArrays.fill(10.0, 2048)
    #     x = map(e -> e / 10, x)
    #     res1 = collect(sum(x))
    #     res2 = collect(minimum(x))

    #     @test typeof(res1) == Float64
    #     @test res1 == 2048
    #     @test typeof(res2) == Float64
    #     @test res2 == 1.0
    # end

    # run_with_job("Multiple evaluations together") do job
    #     x = BanyanArrays.fill(10.0, 2048)
    #     x = map(e -> e / 10, x)
    #     res1 = sum(x)
    #     res2 = minimum(x)

    #     res1 = collect(res1)
    #     res2 = collect(res2)
    #     @test typeof(res1) == Float64
    #     @test res1 == 2048
    #     @test typeof(res2) == Float64
    #     @test res2 == 1.0
    # end

    # TODO: Test HDF5 from URL and from S3
end
