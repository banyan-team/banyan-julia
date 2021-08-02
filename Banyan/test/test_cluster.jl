using AWSCore, AWSS3, HTTP

include("../src/clusters.jl")


# Test `clusters.jl:load_json`
function test_load_json()
    # Test failure if filename is not valid
    @test_throws ErrorException Banyan.load_json("res/Banyanfile.json")

    # Test failure if local file does not exist
    @test_throws ErrorException Banyan.load_json("file://res/filedoesnotexist.json")
    # Test valid local file can be loaded
    banyanfile = Banyan.load_json("file://res/Banyanfile.json")
    @test typeof(banyanfile) <: Dict

    # Test failure if s3 file does not exist
    # TODO: Add this
    # Test valid s3 file can be loaded
    # TODO: Add this

    # Test failure if http(s) file does not exist
    @test_throws HTTP.ExceptionRequest.StatusError Banyan.load_json("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/filedoesnotexist.json")
    # Test valid http(s) file can be loaded
    banyanfile = Banyan.load_json("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/Banyanfile.json")
    @test typeof(banyanfile) <: Dict

end


# Test `clusters.jl:load_file`
function test_load_file()
    # Test failure if filename is not valid
    @test_throws ErrorException Banyan.load_file("res/code_dep.jl")

    # Test failure if local file does not exist
    @test_throws ErrorException Banyan.load_file("file://res/filedoesnotexist.jl")
    # Test valid local json file can be loaded
    f = Banyan.load_file("file://res/Banyanfile.json")
    @test typeof(f) == String
    # Test valid local julia file can be loaded
    f = Banyan.load_file("file://res/code_dep.jl")
    @test typeof(f) == String

    # Test failure if s3 file does not exist
    # TODO: Add this
    # Test valid s3 file can be loaded
    # TODO: Add this

    # Test failure if http(s) file does not exist
    @test_throws HTTP.ExceptionRequest.StatusError Banyan.load_file("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/filedoesnotexist.json")
    # Test valid http(s) file can be loaded
    f = Banyan.load_file("https://raw.githubusercontent.com/banyan-team/banyan-julia/v0.1.0/Banyan/test/res/Banyanfile.json")
    @test typeof(f) == String
end


# Test `clusters.jl:merge_banyanfile_with_defaults`
function test_merge_banyanfile_with_defaults()
    # Test correct result when some parts are specified
    banyanfile_path = "file://res/Banyanfile_a.json"
    banyanfile = Banyan.load_json(banyanfile_path)
    Banyan.merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)
    banyan_dir = dirname(dirname(pathof(Banyan)))
    @test banyanfile == Dict(
        "include" => [],
	"require" => Dict(
	    "language" => "jl",
	    "cluster" => Dict(
                "files" => [],
		"scripts" => [],
		"packages" => ["Distributions"],
		"pt_lib_info" => "file://$banyan_dir/res/pt_lib_info.json",
		"pt_lib" => "file://$banyan_dir/res/pt_lib.jl"
	    ),
	    "job" => Dict(
                "code" => [
	            "using Distributions"
		]
	    )
	)
    )
end


# Test `clusters.jl:merge_banyanfile_with`
function test_merge_banyanfile_with()
    # Test that three banyanfiles can be merged
    banyanfile_path = "file://res/Banyanfile_c.json"
    banyanfile = Banyan.load_json(banyanfile_path)
    Banyan.merge_banyanfile_with_defaults!(banyanfile, banyanfile_path)
    for included in banyanfile["include"]
        Banyan.merge_banyanfile_with!(banyanfile, included, :cluster, :creation)
    end
    banyan_dir = dirname(dirname(pathof(Banyan)))
    @test banyanfile["include"] == []
    @test banyanfile["require"]["cluster"] == Dict(
	"language" => "jl",
        "packages" => ["Distributions", "Plots"],
	"files" => ["file://res/code_dep.jl"],
	"scripts" => [],
	"pt_lib" => "file://$banyan_dir/res/pt_lib.jl",
	"pt_lib_info" => "file://$banyan_dir/res/pt_lib_info.json"
    )
end


# Test `clusters.jl:merge_with`
function test_merge_with()
    @test sort(Banyan.merge_with(
        Dict(
	    "item" => [1, 2, 3]
        ),
	Dict(
	    "item" => [3, 4, 5]
	),
	b -> b["item"]
    )) == [1, 2, 3, 4, 5]
    @test sort(Banyan.merge_with(
        Dict(
	    "item" => ["file1.jl"],
        ),
	Dict(
	    "item" => ["file2.jl"],
	),
	b -> b["item"]
    )) == ["file1.jl", "file2.jl"]
end


# Test `clusters.jl:merge_paths_with`
function test_merge_paths_with()
    # Test raises exception if there are multiple files with same basename
    @test_throws ErrorException sort(Banyan.merge_paths_with(
        Dict(
	    "files" => ["file://res/file1.jl"]
	),
	"",
	Dict(
	    "files" => ["file://file1.jl"]
	),
	b -> b["files"]
    ))
    # Test success
    @test sort(Banyan.merge_paths_with(
        Dict(
	    "files" => ["file://res/file1.jl"]
	),
	"",
	Dict(
	    "files" => ["file://res/file2.jl"]
	),
	b -> b["files"]
    )) == ["file://res/file1.jl", "file://res/file2.jl"]
end


# Test `clusters.jl:keep_same`
function test_keep_same()
    @test Banyan.keep_same(
        Dict(
	    "language" => "jl"
	),
	Dict(
	    "language" => "py"
	),
	b -> b["language"]
    ) == "jl"
end


# Test `clusters.jl:keep_same_path`
function test_keep_same_path()
    @test Banyan.keep_same_path(
        Dict(
	    "pt_lib" => "file://res/pt_lib.jl"
	),
	"",
	Dict(
	    "pt_lib" => "file://res/pt_lib_2.jl"
	),
	b -> b["pt_lib"]
    ) == "file://res/pt_lib.jl"
    @test Banyan.keep_same_path(
        Dict(
	    "pt_lib" => nothing
	),
	"",
	Dict(
	    "pt_lib" => "file://res/pt_lib_2.jl"
	),
	b -> b["pt_lib"]
    ) == "file://res/pt_lib_2.jl"
end


# Test `clusters.jl:upload_banyanfile`
function test_upload_banyanfile()
    # Create bucket for testing. Bucket is always deleted in catch/finally block.
    bucket_name = "banyan-test-bucket-2021"
    s3_create_bucket(Banyan.get_aws_config(), bucket_name)
    try
        # Upload a banyanfile and ensure that the correct files are in the s3 bucket
        Banyan.upload_banyanfile(
            "file://res/Banyanfile.json",
	    s3_arn(bucket_name),
            "banyan-test-cluster-2021",
	    :creation
        )
        objects = sort([obj["Key"] for obj in s3_list_objects(Banyan.get_aws_config(), bucket_name)])
        local_files = ["file://../res/build_hdf5_jl.sh", "file://../res/pt_lib.jl", "file://../res/utils.jl"]
        @test objects == ["banyan_banyan-test-cluster-2021_script.sh", "build_hdf5_jl.sh", "pt_lib.jl", "utils.jl"]
        s3_get(Banyan.get_aws_config(), bucket_name, objects[1])
        @test String(s3_get(Banyan.get_aws_config(), bucket_name, objects[2])) == Banyan.load_file(local_files[1])
        @test String(s3_get(Banyan.get_aws_config(), bucket_name, objects[3])) == Banyan.load_file(local_files[2])
        @test String(s3_get(Banyan.get_aws_config(), bucket_name, objects[4])) == Banyan.load_file(local_files[3])
    catch
        for obj in s3_list_objects(Banyan.get_aws_config(), bucket_name)
            s3_delete(Banyan.get_aws_config(), bucket_name, obj["Key"])
        end
        s3_delete_bucket(Banyan.get_aws_config(), bucket_name)
        rethrow()
    finally
        for obj in s3_list_objects(Banyan.get_aws_config(), bucket_name)
            s3_delete(Banyan.get_aws_config(), bucket_name, obj["Key"])
        end
        s3_delete_bucket(Banyan.get_aws_config(), bucket_name)
    end

    # Test uploading to bucket that doesn't exist
    @test_throws AWSCore.AWSException Banyan.upload_banyanfile(
        "file://res/Banyanfile.json",
	"arn:aws:s3:::filedoesnotexist",
	"no-cluster",
	:creation
    )
end


# Test `clusters.jl:create_cluster` in cases where it should fail
function test_create_cluster_failure_cases()
    
end


# Test `clusters.jl:create_cluster` in cases where it should succeed
function test_create_cluster_success_cases()
end


# Test `clusters.jl:destroy_cluster`
function test_destroy_cluster()
end


# Test `clusters.jl:get_cluster` and `clusters.jl:get_clusters`
function test_get_clusters()
end


# Test `clusters.jl:get_jobs_for_cluster`
function test_get_jobs_for_cluster()
end


# Test `clusters.jl:assert_cluster_is_ready`
function test_assert_cluster_is_ready()
end


# Test `clusters.jl:update_cluster`
function test_update_cluster()
end




@testset "Test loading files" begin
    run("load json") do
        test_load_json()
    end
    run("load file") do
        test_load_file()
    end
end


@testset "Banyanfile" begin
    run("load partial banyanfile") do
	test_merge_banyanfile_with_defaults()
    end
    run("merge banyanfiles") do
        test_merge_banyanfile_with()
        test_merge_with()
        test_merge_paths_with()
        test_keep_same()
        test_keep_same_path()
    end
    run("upload banyanfile") do
        test_upload_banyanfile()
    end
end


@testset "Test creating clusters" begin
    run("create cluster") do
        test_create_cluster_failure_cases()
        test_create_cluster_success_cases()
    end
end


@testset "Test destroying clusters" begin
    run("destroy cluster") do
        test_destroy_cluster()
    end
end


@testset "Test managing clusters" begin
    run("get clusters info") do
        test_get_clusters()
        test_get_jobs_for_cluster()
    end
    run("set cluster status to ready") do
        test_assert_cluster_is_ready()
    end
    run("update cluster") do
        test_update_cluster()
    end
end
