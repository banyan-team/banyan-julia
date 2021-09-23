# Example testing code:
# @testset "Sample collection from $src_name with $scheduling_config" for scheduling_config in
#                                                                         [
#         "default scheduling",
#         "parallelism encouraged",
#         "parallelism and batches encouraged",
#     ],
#     src_name in ["iris_small.parquet", "iris_big.parquet"]
#     use_job_for_testing(scheduling_config_name=scheduling_config)
    
# end
