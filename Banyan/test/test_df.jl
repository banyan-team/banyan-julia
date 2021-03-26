struct FutureDataFrame
    data::Future
    length::Future
    data_sampled::DataFrame
end

function length(df::BanyanDataFrame)

end

function run_iris()
    # TODO: Read in iris
    # TODO: Conver petal length units
    # TODO: Average the petal length
end

@testset "Black Scholes" begin
    run_with_job("Black Scholes", j -> begin
        run_iris()
    end)
end
