function upload_iris_to_s3(bucket_name)
    iris_download_path = "https://raw.githubusercontent.com/Sketchjar/MachineLearningHD/main/iris.csv"
    iris_local_path = download(iris_download_path)
    df = CSV.read(iris_local_path, DataFrames.DataFrame)
    # Duplicate df six times and change the species names
    iris = reduce(vcat, [iris, iris, iris, iris, iris, iris])
    species_list = df[:, :species]
    for i in 4:18
        species_list = append!(species_list, fill("species_$(i)", 50))
    end
    iris[:, :species] = species_list
    CSV.write("iris.csv", df)
    verify_file_in_s3(
        bucket_name,
	"iris_large.csv",
	p"iris.csv",
    )
    verify_file_in_s3(
        bucket_name,
	"iris_species_info.csv",
	"https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris_species_info.csv",
    )
end


@testset "Basic data analytics on a small dataset" begin
    run_with_job("Indexing") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Select rows that have a thin petal (i.e., length > 4 * width). Select only the petal columns.
        res = collect(iris[map((pl, pw) -> pl > 4 * pw, iris[:, :petal_length], iris[:, :petal_width]), :][:, [3, 4]])
        @test round(sum(res[:, :petal_width])) == 61.0
        @test res[1, 1] == 1.4
        @test res[36, 2] == 0.3
        @test res[44, 1] == 4.1
        @test collect(res)[256, [1, 2]] = [1.3, 0.3]
    end

    run_with_job("Filtering small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Filter
        iris_filtered =
            filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), iris)
        iris_filtered_collect = collect(iris_filtered)
        @test nrow(iris_filtered) == 306
        @test iris_filtered_collect[1, :sepal_width] == 2.3
        @test iris_filtered_collect[46, :petal_length] == 6.1
        @test iris_filtered_collect[51, :sepal_length] == 6.3
        @test collect(iris_filtered_collect[298, :]) == [6.3, 2.7, 4.9, 1.8, "species_18"]

        # Unique
        unique_species = collect(unique(iris[:, [:species]]))
        @test unique_species[:, :species] == ["setosa", "versicolor", "virginica", "species_4", "species_5", "species_6", "species_7", "species_8", "species_9", "species_10", "species_11", "species_12", "species_13", "species_14", "species_15", "species_16", "species_17", "species_18"]

        # Nonunique
        @test collect(sum(nonunique(iris))) == 18
        @test findall(collect(nonunique(iris))) == [35, 38, 143, 185, 188, 293, 335, 338, 443, 485, 488, 593, 635, 638, 743, 785, 788, 893]
        nonunique_species = collect(nonunique(iris, :species))
        @test sum(nonunique_species) == 882
        @test nonunique_species[[1, 51, 101]] == [0, 0, 0]

        # Dropmissing, allowmissing, disallowmissing
        @test nrow(dropmissing(iris)) == 900
        iris_allowmissing = allowmissing(iris)
        #setindex!(iris_allowmissing, missing, 1, 1)
        #setindex!(iris_allowmissing, missing, 123, 2)
        #setindex!(iris_allowmissing, missing, 150, 5)
        #iris_cleaned = dropmissing(iris_allowmissing)
        #iris_cleaned = disallowmissing(iris_cleaned)
        #@test nrow(iris_cleaned) == 147
        #@test collect(iris_cleaned[123, :])[:sepal_length] == 6.7
        #iris_cleaned = collect(iris_cleaned)
        #@test first(iris_cleaned)[:sepal_width] == 3.0
        #@test last(iris_cleaned)[:petal_width] == 2.3
    end

    run_with_job("Sorting small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Sort forward and backward
        iris_sorted = sort(iris, :sepal_width)
        iris_sorted_reverse = sort(iris, :sepal_width, rev=true)

        iris_sorted = collect(iris_sorted)
        iris_sorted_reverse = collect(iris_sorted_reverse)
        @test iris_sorted[[1, 142, nrow(iris_sorted)], [:sepal_width, :petal_length]] == DataFrame(:sepal_width=>[2.0, 2.6, 4.4], :petal_length=>[3.5, 4.0, 1.5])
        @test iris_sorted_reverse[[1, 142, nrow(iris_sorted)], [:sepal_width, :petal_length]] == DataFrame(:sepal_width => [4.4, 3.5, 2.0], :petal_length => [1.5, 1.3, 3.5])
    end

    run_with_job("Join and group-by-aggregate small dataset") do job
        # Join and groupby aggregration
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")
        species_info = read_csv("s3://$(bucket)/iris_species_info.csv")
        iris_new = innerjoin(iris, species_info, on = :species)
        @test ncol(iris_new) == 6

        gdf_species = groupby(iris_new, :species)
        gdf_region = groupby(iris_new, :region)

        gr_cols = collect(groupcols(gdf_region))
        gs_cols = collect(groupcols(gdf_species))
        vr_cols = collect(valuecols(gdf_region))

        @test gr_cols == [:region]
        @test gs_cols == [:species]
        @test vr_cols == [:sepal_length, sepal_width, :petal_length, :petal_width, :species]

        lengths_species = collect(combine(gdf_species, :petal_length => mean))
        counts_species = collect(combine(gdf_region, nrow))
        lengthis_region = collect(combine(gdf_region, :petal_length => mean))
        counts_region = collect(combine(gdf_region, nrow))

        @test lengths_species[:, :petal_length_mean] ==
              lengths_region[:, :petal_length_mean] ==
              [1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552]
        @test counts_species[:, :nrow] == counts_region[:, :nrow] == fill(50, 18)
    end

    run_with_job("Group and map over small dataset") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Prepare some larger data. Groupby by rounded petal length into 54 groups.
        iris = innerjoin(iris, iris[:, [:species]], on = :species)
        setindex!(iris, :petal_length_rounded, round.(iris[:, :petal_length]))
        gdf = groupby(iris, [:species, :petal_length_rounded])
        @test length(gdf) == 54

        # Select
        iris_select = select(gdf, :, [:petal_length] => (pl) -> pl .- mean(pl))
        @test round.(
            collect(iris_select[:, :petal_length_function][[1, 7488, 34992, nrow(iris_select)]]),
            digits = 3,
        ) == [0.078, -0.308, -0.171, 0.014]

        # Transform
        iris_new = transform(gdf, :species => x -> "iris-" .* x)
        species_names = collect(unique(iris_new[:, "species_function"]))
        @test species_names == ["iris-setosa", "iris-versicolor", "iris-virginica", "iris-species_4", "iris-species_5", "iris-species_6", "iris-species_7", "iris-species_8", "iris-species_9", "iris-species_10", "iris-species_11", "iris-species_12", "iris-species_13", "iris-species_14", "iris-species_15", "iris-species_16", "iris-species_17", "iris-species_18"]

        # Split-Apply-Combine
        iris_mins = collect(combine(gdf, :petal_length => minimum))
        @test iris_mins[:, :petal_length_minimum] ==
              [1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5, 1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5, 1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5, 1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5, 1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5, 1.0, 1.5, 4.6, 3.5, 3.0, 5.5, 4.8, 6.6, 4.5]

        # Subset
        long_petal_iris = combine(
            groupby(subset(gdf, :petal_length => pl -> pl .>= mean(pl)), :species),
            nrow,
        )
        @test collect(long_petal_iris)[:, :nrow] == [1250, 1250, 1200, 1250, 1250, 1200, 1250, 1250, 1200, 1250, 1250, 1200, 1250, 1250, 1200, 1250, 1250, 1200]
        @test collect(long_petal_iris)[:, :species] == ["setosa", "versicolor", "virginica", "species_4", "species_5", "species_6", "species_7", "species_8", "species_9", "species_10", "species_11", "species_12", "species_13", "species_14", "species_15", "species_16", "species_17", "species_18"]
    end

    run_with_job("Simple group-by aggregation") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")
        gdf = groupby(iris, :species)
        lengths = collect(combine(gdf, :petal_length => mean))
        counts = collect(combine(gdf, nrow))

        @show lengths
        @show counts

        @test length(gdf) == 18
        @test lengths[:, :petal_length_mean] == [1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552, 1.464, 4.26, 5.552]
        @test counts[:, :nrow] == fill(50, 18)
    end
end

@testset "Complex data analytics on a small dataset" begin
    run_with_job("Multiple evaluations together - test 1") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Compute the min-max normalized petal_length (within each species)
        # for each flower and sort by this value. Select those with normalized
        # petal length greater than 0.5.

        gdf = groupby(iris, :species)

        # Method 1
        result_1 = transform(
            gdf,
            :petal_length => x -> (x .- minimum(x)) ./ (maximum(x) - minimum(x)),
        )
        result_1 = sort(result_1, :petal_length_function)
        result_1 = rename(result_1, :petal_length_function => :petal_length_normalized)
        result_1 = filter(row -> row.petal_length_normalized < row.petal_length, result_1)

        # Method 2
        min_max = combine(gdf, :petal_length => minimum, :petal_length => maximum)
        iris_new = innerjoin(iris, min_max, on = :species)
        iris_new[:, :petal_length_normalized] = map(
	    (l, min, max) -> (pl - min) / (max - min),
	    iris_new[:, :petal_length],
	    iris_new[:, :petal_length_minimum],
	    iris_new[:, :petal_length_maximum]
	)
        result_2 =
            sort(iris_new, :petal_length_normalized)
        result_2 = filter(row -> row.petal_length_normalized < row.petal_length, result_2)

        @test nrow(result_1) == nrow(result_2) == 462

        result_1 = collect(result_1)
        result_2 = collect(result_2)

        @test isapprox(result_1[7, :petal_length_normalized], 0.52380, atol = 1e-4)
        @test isapprox(result_2[7, :petal_length_normalized], 0.52380, atol = 1e-4)
        @test isapprox(result_1[200, :petal_length_normalized], 0.66666, atol = 1e-4)
        @test isapprox(result_2[200, :petal_length_normalized], 0.66666, atol = 1e-4)
        @test first(result_1)[:species] == first(result_2)[:species] == "versicolor"
        @test last(result_1)[:species] == last(result_2)[:species] == "species_18"
        @test last(result_1)[:petal_length_normalized] ==
              last(result_1)[:petal_length_normalized] ==
              1.0
    end

    run_with_job("Multiple evaluations together - test 2") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Inner join with itself on species to increase cardinality. Group by rounded sepal length
        # and compute number of rows. Sort by number of rows.
        iris = innerjoin(iris, iris[:, [:species]], on = :species)
        iris[:, :sepal_length_function] = round.(iris[:, :sepal_length])
        iris_sepal_length_groups = compute(
            sort(
                combine(
                    groupby(
                        rename(iris, :sepal_length_function => :sepal_length_rounded),
                        :sepal_length_rounded,
                    ),
                    nrow,
                ),
                :nrow,
            ),
        )
        @test collect(iris_sepal_length_groups[:, :nrow]) == [250, 300, 1200, 2350, 3400]
        @test collect(iris_sepal_length_groups[:, :nrow]) == [4.0, 8.0, 7.0, 5.0, 6.0]
    end

    run_with_job("Multiple evaluations apart - test 1") do job
        bucket = get_cluster_s3_bucket_name(get_cluster().name)
        upload_iris_to_s3(bucket)
        iris = read_csv("s3://$(bucket)/iris_large.csv")

        # Inner join twice with itself on species to increase cardinality. Group by species and rounded petal length.
        # Compute cardinality of each group.
        # Also compute deviation from mean of petal_width of each group, filter for those with a deviation less that +/- 0.2,
        # and group on species.
        iris_large = groupby(
            innerjoin(
                innerjoin(iris, iris[:, [:species]], on = :species),
                iris[:, [:species]],
                on = :species,
            ),
            :species,
        )
        iris_large_gdf = groupby(
            rename(
                select(
                    iris_large,
                    :,
                    [:species, :petal_length] =>
                        (s, pl) -> s .* "-pl=" .* string.(round.(pl)),
                ),
                :species_petal_length_function => :species_pl,
            ),
            :species_pl,
        )
        @test length(iris_large_gdf) == 54

        lengths = collect(combine(iris_large_gdf, nrow))
        @test lengths[10, "species_pl"] == "species_4-pl=1.0"
        @test lengths[:, :nrow] ==
              [57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500, 57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500, 57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500, 57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500, 57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500, 57500, 67500, 35000, 82500, 7500, 60000, 52500, 10000, 2500]

        iris_large_dev = sort(
            transform(iris_large_gdf, :petal_width => w -> (mean(w) .- w)),
            :petal_width_function,
        )
        lengths_dev = collect(
            filter(
                row -> (
                    row.petal_width_function >= -0.2 && row.petal_width_function <= 0.2
                ),
                iris_large_dev,
            ),
        )
        @test nrow(lengths_dev) = 1650000
    end
end

