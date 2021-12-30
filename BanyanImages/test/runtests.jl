using Banyan, BanyanImages
using ReTest
using AWSS3, FileIO, ImageIO, ImageCore, Random


include("locations.jl")


try
    runtests(Regex.(ARGS)...)
finally
    # Destroy jobs to clean up.
    # destroy_all_jobs_for_testing()
end