struct FutureDataFrame
    data::Future
    length::Future
    # data_sampled::DataFrame
end

function read_csv(pathname)
    data = Future()
    len = Future()

    location = CSV(pathname)
    src(data, location)
    val(length, location.nrows)

    pt(data, BlockBalanced())
    pt(len, Div())
    mut(data)

    @partitioned begin end

    FutureDataFrame(data, length)
end

function length(df::BanyanDataFrame)

end

function run_iris()
    # TODO: Read in iris
    # TODO: Conver petal length units
    # TODO: Average the petal length
    read_csv("s3://banyanexecutor/iris.csv")
end

@testset "Black Scholes" begin
    run_with_job("Black Scholes", j -> begin
        run_iris()
    end)
end
