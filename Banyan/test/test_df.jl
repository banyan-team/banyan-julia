struct FutureDataFrame
    data::Future
    len::Future
    # data_sampled::DataFrame
end

Banyan.future(ba::T) where {T<:FutureDataFrame} = ba.data

function read_csv(pathname)
    location = CSVPath(pathname)

    data = Future()
    len = Future(location.nrows)
    
    src(data, location)
    val(len)

    pt(data, Block())
    # pt(len, Replicate())

    @partitioned data begin end

    FutureDataFrame(data, len)
end

function write_csv(df::FutureDataFrame, pathname)
    dst(df, CSVPath(pathname))
    pt(df, Block())
    mut(df)
    @partitioned df begin end
end

length(df::FutureDataFrame) = evaluate(df.len)

# function select()

function run_iris()
    # TODO: Read in iris
    # TODO: Conver petal length units
    # TODO: Average the petal length
    # df = read_csv("s3://banyanexecutor/iris.csv")
    df = read_csv("s3://banyan-cluster-data-mycluster/datasets/input/")
    write_csv(df, "s3://banyan-cluster-data-mycluster/datasets/output/")
    println(evaluate(df.len))

    evaluate(df)
end

@testset "Iris" begin
    run_with_job("Iris", j -> begin
        run_iris()
    end)
end
