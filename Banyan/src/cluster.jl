struct Cluster
    name::String
    status::Symbol
    status_explanation::String
    num_jobs_running::Int32
    s3_bucket_arn::String
end