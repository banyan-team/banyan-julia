# function upload_iris_to_s3(bucket_name)
#     # iris_download_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
#     iris_download_path = "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris.csv"
#     iris_local_path = download(iris_download_path)
#     df = CSV.read(iris_local_path, DataFrames.DataFrame)
#     # Duplicate df six times and change the species names
#     species_list = df[:, :species]
#     df = reduce(vcat, [df, df, df, df, df, df])
#     for i = 4:18
#         species_list = append!(species_list, Base.fill("species_$(i)", 50))
#     end
#     df[:, :species] = species_list
#     CSV.write("iris.csv", df)
#     verify_file_in_s3(bucket_name, "iris_large.csv", p"iris.csv")
#     verify_file_in_s3(
#         bucket_name,
#         "iris_species_info.csv",
#         "https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.3/BanyanDataFrames/test/res/iris_species_info.csv",
#     )
# end

# # TODO: Add test for setting multiple columns with a matrix


# @testset "Basic data analytics on a small dataset" begin
#     run_with_session("Indexing small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Select rows that have a thin petal (i.e., length > 4 * width). Select only the petal columns.
#         res = collect(
#             sort(
#                 iris[
#                     map(
#                         (pl, pw) -> pl > 4 * pw,
#                         iris[:, :petal_length],
#                         iris[:, :petal_width],
#                     ),
#                     :,
#                 ][
#                     :,
#                     [3, 4],
#                 ],
#             ),
#         )
#         @test round(sum(res[:, :petal_width])) == 61.0
#         @test res[1, 1] == 1.0
#         @test res[36, 2] == 0.2
#         @test res[44, 1] == 1.3
#         res_row_1 = res[256, [1, 2]]
#         @test collect(res_row_1) == [1.9, 0.4]
#         res_row_2 = res[nrow(res), [1, 2]]
#         @test collect(res_row_2) == [4.1, 1.0]
#     end

#     run_with_session("Filtering small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Filter
#         iris_filtered =
#             sort(filter(row -> (row.sepal_length > 5.0 && row.sepal_width < 3.0), iris))
#         iris_filtered_collect = collect(iris_filtered)
#         @test nrow(iris_filtered) == 306
#         @test iris_filtered_collect[1, :sepal_width] == 2.5
#         @test iris_filtered_collect[46, :petal_length] == 3.9
#         @test iris_filtered_collect[51, :sepal_length] == 5.6
#         @test collect(iris_filtered_collect[298, :]) == [7.7, 2.6, 6.9, 2.3, "species_6"]

#         # Unique
#         @test nrow(unique(iris[:, [:species]])) == 18
#         unique_iris = sort(unique(iris))
#         @test nrow(unique_iris) == 882
#         @test nrow(
#             unique(iris[:, [:sepal_length, :sepal_width, :petal_length, :petal_width]]),
#         ) == 147

#         # Nonunique before aggregation
#         nonunique_idxs = nonunique(iris)
#         nonunique_iris = collect(sort(iris[nonunique_idxs, :]))
#         @show collect(iris[nonunique_idxs, :])
#         @show nonunique_iris
#         @show iris
#         @show typeof(iris)
#         @show collect(iris)
#         @show collect(nonunique_idxs)
#         @test collect(first(nonunique_iris)) == [4.9, 3.1, 1.5, 0.1, "setosa"]
#         @test collect(nonunique_iris[10, :]) == [4.9, 3.1, 1.5, 0.1, "species_4"]
#         @test collect(last(nonunique_iris)) == [5.8, 2.7, 5.1, 1.9, "virginica"]
#         nonunique_species = collect(nonunique(iris, :species))
#         @test sum(nonunique_species) == 882

#         # Nonunique after aggregation
#         @test collect(sum(nonunique_idxs)) == 18
#         # @show collect(sum(nonunique_idxs))
#         # @show sample(nonunique_idxs)
#         # @show collect(nonunique_idxs)
#         nonunique_iris = collect(sort(iris[nonunique_idxs, :]))
#         @show collect(iris[nonunique_idxs, :])
#         @show nonunique_iris
#         @show collect(iris)
#         @show collect(nonunique_idxs)
#         @test collect(first(nonunique_iris)) == [4.9, 3.1, 1.5, 0.1, "setosa"]
#         @test collect(nonunique_iris[10, :]) == [4.9, 3.1, 1.5, 0.1, "species_4"]
#         @test collect(last(nonunique_iris)) == [5.8, 2.7, 5.1, 1.9, "virginica"]
#         nonunique_species = collect(nonunique(iris, :species))
#         @test sum(nonunique_species) == 882

#         # Nonunique after _another_ aggregation
#         @test collect(sum(nonunique_idxs)) == 18
#         # @show collect(sum(nonunique_idxs))
#         # @show sample(nonunique_idxs)
#         # @show collect(nonunique_idxs)
#         nonunique_iris = collect(sort(iris[nonunique_idxs, :]))
#         @show collect(iris[nonunique_idxs, :])
#         @show nonunique_iris
#         @show collect(iris)
#         @show collect(nonunique_idxs)
#         @test collect(first(nonunique_iris)) == [4.9, 3.1, 1.5, 0.1, "setosa"]
#         @test collect(nonunique_iris[10, :]) == [4.9, 3.1, 1.5, 0.1, "species_4"]
#         @test collect(last(nonunique_iris)) == [5.8, 2.7, 5.1, 1.9, "virginica"]
#         nonunique_species = collect(nonunique(iris, :species))
#         @test sum(nonunique_species) == 882

#         # Dropmissing, allowmissing, disallowmissing
#         @test nrow(dropmissing(iris)) == 900
#         iris_allowmissing = allowmissing(iris)
#         #setindex!(iris_allowmissing, missing, 1, 1)
#         #setindex!(iris_allowmissing, missing, 123, 2)
#         #setindex!(iris_allowmissing, missing, 150, 5)
#         #iris_cleaned = dropmissing(iris_allowmissing)
#         #iris_cleaned = disallowmissing(iris_cleaned)
#         #@test nrow(iris_cleaned) == 147
#         #@test collect(iris_cleaned[123, :])[:sepal_length] == 6.7
#         #iris_cleaned = collect(iris_cleaned)
#         #@test first(iris_cleaned)[:sepal_width] == 3.0
#         #@test last(iris_cleaned)[:petal_width] == 2.3
#     end

#     run_with_session("Sorting small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Sort forward and backward
#         iris_sorted = sort(iris, :sepal_width)
#         iris_sorted_reverse = sort(iris, :sepal_width, rev = true)

#         iris_sorted = collect(iris_sorted)
#         iris_sorted_reverse = collect(iris_sorted_reverse)

#         @test iris_sorted[[1, 142, nrow(iris_sorted)], [:sepal_width, :petal_length]] ==
#               DataFrames.DataFrame(:sepal_width => [2.0, 2.6, 4.4], :petal_length => [3.5, 4.0, 1.5])
#         @test iris_sorted_reverse[
#             [1, 142, nrow(iris_sorted)],
#             [:sepal_width, :petal_length],
#         ] == DataFrames.DataFrame(:sepal_width => [4.4, 3.5, 2.0], :petal_length => [1.5, 1.3, 3.5])
#     end

#     run_with_session("Join and group-by-aggregate small dataset") do session
#         # Join and groupby aggregration
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")
#         species_info = read_csv("s3://$(bucket)/iris_species_info.csv")
#         iris_new = innerjoin(iris, species_info, on = :species)
#         @test ncol(iris_new) == 6

#         gdf_species = groupby(iris_new, :species)
#         gdf_region = groupby(iris_new, :region)

#         gr_cols = collect(groupcols(gdf_region))
#         gs_cols = collect(groupcols(gdf_species))
#         vr_cols = Set(collect(valuecols(gdf_region)))

#         @test gr_cols == [:region]
#         @test gs_cols == [:species]
#         @test vr_cols ==
#               Set([:sepal_length, :sepal_width, :petal_length, :petal_width, :species])

#         lengths_species =
#             collect(sort(combine(gdf_species, :petal_length => mean), :petal_length_mean))
#         counts_species = collect(sort(combine(gdf_region, nrow), :region))
#         lengths_region =
#             collect(sort(combine(gdf_region, :petal_length => mean), :petal_length_mean))
#         counts_region = collect(sort(combine(gdf_region, nrow), :region))

#         @test lengths_species[:, :petal_length_mean] ==
#               lengths_region[:, :petal_length_mean] ==
#               [
#                   1.464,
#                   1.464,
#                   1.464,
#                   1.464,
#                   1.464,
#                   1.464,
#                   4.26,
#                   4.26,
#                   4.26,
#                   4.26,
#                   4.26,
#                   4.26,
#                   5.552,
#                   5.552,
#                   5.552,
#                   5.552,
#                   5.552,
#                   5.552,
#               ]
#         @test counts_species[:, :nrow] == counts_region[:, :nrow] == Base.fill(50, 18)
#     end

#     run_with_session("Group and map over small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Prepare some larger data. Groupby by rounded petal length into 54 groups.
#         iris = innerjoin(iris, iris[:, [:species]], on = :species)
#         setindex!(iris, map(pl -> round(pl), iris[:, :petal_length]), :, :petal_length_rounded)
#         gdf = groupby(iris, [:species, :petal_length_rounded])
#         @test length(gdf) == 54

#         # Select
#         iris_select = sort(
#             select(gdf, :, [:petal_length] => (pl) -> pl .- mean(pl)),
#             :petal_length_function,
#         )
#         petal_length_function_sum = round(collect(reduce(-, iris_select[:, :petal_length_function])), digits=2)
#         @test petal_length_function_sum == -52.71
        
#         #@test round.(
#         #    collect(
#         #        iris_select[:, :petal_length_function][[1, 7488, 34992, nrow(iris_select)]],
#         #    ),
#         #    digits = 3,
#         #) == [-0.639, -0.186, 0.115, 0.592]

#         # Transform
#         iris_new = transform(gdf, :species => x -> "iris-" .* x)
#         species_names = collect(sort(unique(iris_new[:, "species_function"])))
#         @test species_names == [
#             "iris-setosa",
#             "iris-species_10",
#             "iris-species_11",
#             "iris-species_12",
#             "iris-species_13",
#             "iris-species_14",
#             "iris-species_15",
#             "iris-species_16",
#             "iris-species_17",
#             "iris-species_18",
#             "iris-species_4",
#             "iris-species_5",
#             "iris-species_6",
#             "iris-species_7",
#             "iris-species_8",
#             "iris-species_9",
#             "iris-versicolor",
#             "iris-virginica",
#         ]

#         # Split-Apply-Combine
#         iris_mins =
#             collect(sort(combine(gdf, :petal_length => minimum), :petal_length_minimum))
#         @test iris_mins[:, :petal_length_minimum] == [
#             1.0,
#             1.0,
#             1.0,
#             1.0,
#             1.0,
#             1.0,
#             1.5,
#             1.5,
#             1.5,
#             1.5,
#             1.5,
#             1.5,
#             3.0,
#             3.0,
#             3.0,
#             3.0,
#             3.0,
#             3.0,
#             3.5,
#             3.5,
#             3.5,
#             3.5,
#             3.5,
#             3.5,
#             4.5,
#             4.5,
#             4.5,
#             4.5,
#             4.5,
#             4.5,
#             4.6,
#             4.6,
#             4.6,
#             4.6,
#             4.6,
#             4.6,
#             4.8,
#             4.8,
#             4.8,
#             4.8,
#             4.8,
#             4.8,
#             5.5,
#             5.5,
#             5.5,
#             5.5,
#             5.5,
#             5.5,
#             6.6,
#             6.6,
#             6.6,
#             6.6,
#             6.6,
#             6.6,
#         ]

#         # Subset
#         long_petal_iris = sort(
#             combine(
#                 groupby(subset(gdf, :petal_length => pl -> pl .>= mean(pl)), :species),
#                 nrow,
#             ),
#         )
#         long_petal_iris_nrow = collect(long_petal_iris)[:, :nrow]
#         long_petal_iris_species = collect(long_petal_iris)[:, :species]
#         @test long_petal_iris_nrow == [
#             1250,
#             1250,
#             1250,
#             1200,
#             1250,
#             1250,
#             1200,
#             1250,
#             1250,
#             1200,
#             1250,
#             1250,
#             1200,
#             1250,
#             1250,
#             1200,
#             1250,
#             1200,
#         ]
#         @test long_petal_iris_species == [
#             "setosa",
#             "species_10",
#             "species_11",
#             "species_12",
#             "species_13",
#             "species_14",
#             "species_15",
#             "species_16",
#             "species_17",
#             "species_18",
#             "species_4",
#             "species_5",
#             "species_6",
#             "species_7",
#             "species_8",
#             "species_9",
#             "versicolor",
#             "virginica",
#         ]
#     end

#     run_with_session("Simple group-by aggregation on small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")
#         gdf = groupby(iris, :species)
#         lengths = collect(sort(combine(gdf, :petal_length => mean)))
#         counts = collect(combine(gdf, nrow))

#         @show lengths
#         @show counts

#         @test length(gdf) == 18
#         @test lengths[:, :petal_length_mean] == [
#             1.464,
#             1.464,
#             4.26,
#             5.552,
#             1.464,
#             4.26,
#             5.552,
#             1.464,
#             4.26,
#             5.552,
#             1.464,
#             4.26,
#             5.552,
#             1.464,
#             4.26,
#             5.552,
#             4.26,
#             5.552,
#         ]
#         @test counts[:, :nrow] == Base.fill(50, 18)
#     end

#     run_with_session("Inner Join") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")
#         species_info = read_csv("s3://$(bucket)/iris_species_info.csv")

#         # Rename columns of species_info
#         iris_species_info =
#             rename(species_info, :species => :species_info, :region => :region_info)

#         # Join iris and species_info on the :species/:species_info column
#         iris_joined = innerjoin(
#             iris,
#             iris_species_info,
#             on = :species => :species_info,
#             renamecols = uppercase => uppercase,
#         )
#         @test Set(names(iris_joined)) == Set([
#             "SEPAL_WIDTH",
#             "PETAL_WIDTH",
#             "REGION_INFO",
#             "PETAL_LENGTH",
#             "SEPAL_LENGTH",
#             "species",
#         ])
#         @test nrow(iris_joined) == 900

#         # Join iris on multiple columns
#         iris[:, :petal_length_rounded] = map(pl -> round(pl), iris[:, :petal_length])
#         species_info[:, :pl_rounded] =
#             [1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4]
#         iris_joined = sort(
#             innerjoin(
#                 iris,
#                 species_info,
#                 on = [:species, :petal_length_rounded => :pl_rounded],
#                 :sepal_length,
#             ),
#         )
#         @test Set(names(iris_joined)) == Set([
#             "sepal_length",
#             "sepal_width",
#             "petal_length",
#             "petal_width",
#             "species",
#             "petal_length_rounded",
#             "region",
#         ])
#         @test nrow(iris_joined) == 146
#         res = collect(iris_joined)
#         @test collect(res[1, :]) == [4.3, 3.0, 1.1, 0.1, "setosa", 1.0, "Arctic"]
#         @test collect(res[121, :]) == [6.6, 2.9, 4.6, 1.3, "species_5", 5.0, "region_5"]
#         @test collect(res[146, :]) == [7.9, 3.8, 6.4, 2.0, "species_6", 6.0, "region_6"]
#     end
# end

# @testset "Complex data analytics on a small dataset" begin
#     run_with_session("Multiple evaluations together (test 1) on small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Compute the min-max normalized petal_length (within each species)
#         # for each flower and sort by this value. Select those with normalized
#         # petal length greater than 0.5.

#         gdf = groupby(iris, :species)

#         # Method 1
#         result_1 = transform(
#             gdf,
#             :petal_length => x -> (x .- minimum(x)) ./ (maximum(x) - minimum(x)),
#         )
#         result_1 = rename(result_1, :petal_length_function => :petal_length_normalized)
#         result_1 = filter(row -> row.petal_length_normalized < row.petal_length, result_1)
#         result_1 = sort(result_1, :petal_length_normalized)

#         # Method 2
#         min_max = combine(gdf, :petal_length => minimum, :petal_length => maximum)
#         iris_new = innerjoin(iris, min_max, on = :species)
#         iris_new[:, :petal_length_normalized] = map(
#             (l, min, max) -> (pl - min) / (max - min),
#             iris_new[:, :petal_length],
#             iris_new[:, :petal_length_minimum],
#             iris_new[:, :petal_length_maximum],
#         )
#         result_2 = filter(row -> row.petal_length_normalized < row.petal_length, result_2)
#         result_2 = sort(result_2, :petal_length_normalized)

#         @test nrow(result_1) == nrow(result_2) == 462

#         result_1 = collect(result_1)
#         result_2 = collect(result_2)

#         @test isapprox(result_1[7, :petal_length_normalized], 0.52380, atol = 1e-4)
#         @test isapprox(result_2[7, :petal_length_normalized], 0.52380, atol = 1e-4)
#         @test isapprox(result_1[200, :petal_length_normalized], 0.66666, atol = 1e-4)
#         @test isapprox(result_2[200, :petal_length_normalized], 0.66666, atol = 1e-4)
#         @test first(result_1)[:species] == first(result_2)[:species] == "versicolor"
#         @test last(result_1)[:species] == last(result_2)[:species] == "species_18"
#         @test last(result_1)[:petal_length_normalized] ==
#               last(result_1)[:petal_length_normalized] ==
#               1.0
#     end

#     run_with_session("Multiple evaluations together (test 2) on small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Inner join with itself on species to increase cardinality. Group by rounded sepal length
#         # and compute number of rows. Sort by number of rows.
#         iris = innerjoin(iris, iris[:, [:species]], on = :species)
#         iris[:, :sepal_length_function] = map(sl -> round(sl), iris[:, :sepal_length])
#         iris_sepal_length_groups = write_to_disk(
#             sort(
#                 combine(
#                     groupby(
#                         rename(iris, :sepal_length_function => :sepal_length_rounded),
#                         :sepal_length_rounded,
#                     ),
#                     nrow,
#                 ),
#                 :nrow,
#             ),
#         )
#         res = collect(iris_sepal_length_groups)
#         @test res[:, :nrow] == [1500, 1800, 7200, 14100, 20400]
#         @test res[:, :sepal_length_rounded] == [4.0, 8.0, 7.0, 5.0, 6.0]
#     end

#     run_with_session("Multiple evaluations apart on small dataset") do session
#         bucket = get_cluster_s3_bucket_name(get_cluster_name())
#         # upload_iris_to_s3(bucket)
#         iris = read_csv("s3://$(bucket)/iris_large.csv")

#         # Inner join twice with itself on species to increase cardinality. Group by species and rounded petal length.
#         # Compute cardinality of each group.
#         # Also compute deviation from mean of petal_width of each group, filter for those with a deviation less that +/- 0.2,
#         # and group on species.
#         iris_large = groupby(
#             innerjoin(
#                 innerjoin(iris, iris[:, [:species]], on = :species),
#                 iris[:, [:species]],
#                 on = :species,
#             ),
#             :species,
#         )
#         iris_large_gdf = groupby(
#             rename(
#                 select(
#                     iris_large,
#                     :,
#                     [:species, :petal_length] =>
#                         (s, pl) -> s .* "-pl=" .* string.(round.(pl)),
#                 ),
#                 :species_petal_length_function => :species_pl,
#             ),
#             :species_pl,
#         )
#         @test length(iris_large_gdf) == 54

#         lengths = compute(sort(combine(iris_large_gdf, nrow)))
#         @test lengths[10, "species_pl"] == "species_12-pl=6.0"
#         @test lengths[:, :nrow] == [
#             57500,
#             67500,
#             57500,
#             67500,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#             57500,
#             67500,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#             57500,
#             67500,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#             57500,
#             67500,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#             57500,
#             67500,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#             7500,
#             82500,
#             35000,
#             2500,
#             52500,
#             60000,
#             10000,
#         ]

#         iris_large_dev = sort(
#             transform(iris_large_gdf, :petal_width => w -> (mean(w) .- w)),
#             :petal_width_function,
#         )
#         lengths_dev = compute(
#             filter(
#                 row -> (
#                     row.petal_width_function >= -0.2 && row.petal_width_function <= 0.2
#                 ),
#                 iris_large_dev,
#             ),
#         )
#         @test nrow(lengths_dev) == 1650000
#     end
# end

