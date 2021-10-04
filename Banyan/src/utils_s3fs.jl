get_s3_bucket_arn(cluster_name) = get_cluster(cluster_name).s3_bucket_arn
get_s3_bucket_path(cluster_name) =
    replace(get_cluster(cluster_name).s3_bucket_arn, "arn:aws:s3:::" => "s3://")
function get_s3fs_bucket_path(cluster_name)
    arn = get_cluster(cluster_name).s3_bucket_arn
    joinpath(
        homedir(),
        ".banyan",
        "mnt",
        "s3",
        arn[findfirst("arn:aws:s3:::", arn).stop+1:end],
    )
end

#########################
# MOUNTED S3 FILESYSTEM #
#########################

tmp_paths = []

# You should first call `download_remote_path` to get a remote path, then
# call `with_downloaded_path_for_reading` for the local path to read from.

function download_remote_path(remotepath)
    global tmp_paths

    # This returns an AbstractPath that can be manipulated using any of the
    # `Base.Filesystem` methods. To actually use it for reading, you should
    # first pass the returned `AbstractPath` through
    # `with_downloaded_path_for_reading`. That will ensure that if the path
    # is still referring to an S3 object, it will be first downloaded locally.
    # All files will be deleted afterwards if they are in a temporary
    # directory.

    if startswith(remotepath, "s3://")
        download_remote_s3_path(remotepath)
        # This will either return an `S3Path` still referring to a remote
        # location or it will return a local S3FS path for reading from.
    elseif startswith(remotepath, "http://") || startswith(remotepath, "https://")
        tmp_path = tempname() * splitext(remotepath)[2]
        push!(tmp_paths, tmp_path)
        Downloads.download(remotepath, tmp_path)
    else
        throw(
            ArgumentError(
                "$remotepath does not start with either http:// or https:// or s3://",
            ),
        )
    end
end

failed_to_use_s3fs = false

function download_remote_s3_path(path)
    global failed_to_use_s3fs

    # Get information about requested object
    s3path = S3Path(path, config = get_aws_config())
    bucket = s3path.bucket
    key = s3path.key
    # bucket = "banyan-cluster-data-myfirstcluster"
    mount = joinpath(homedir(), ".banyan", "mnt", "s3", bucket)

    # If specified, do not allow even attempting to use S3FS.
    if haskey(ENV, "BANYAN_USE_S3FS") && ENV["BANYAN_USE_S3FS"] == "0"
        failed_to_use_s3fs = true
    end

    # Don't attempt to use S3FS if we have ever failed to use S3FS.
    if !failed_to_use_s3fs
        # Ensure path to mount exists
        no_mount = false
        try
            if !isdir(mount)
                mkpath(mount)
                # TODO: Ensure that no directory really means there is no mount
                no_mount = true
                @warn "Attempting to remount S3FS because no directory found at $mount"
            end
            if !ismount(mount)
                no_mount = true
                @warn "Attempting to remount S3FS because no mount found at $mount"
            end
        catch
            no_mount = true
            @warn "Attempting to remount S3FS because attempting to stat the directory at $mount failed"
        end

        # Ensure something is mounted
        if no_mount
            try
                run(`umount -fq $mount`)
            catch e
                @warn "Failed to re-mount S3FS with error: $e. You may try to force unmounting with \`umount -fq $mount\` and then re-run."
            end

            # TODO: Store buckets from different accounts/IAMs/etc. seperately
            try
                ACCESS_KEY_ID = get_aws_config()[:creds].access_key_id
                SECRET_ACCESS_KEY = get_aws_config()[:creds].secret_key
                passwd_s3fs_contents = ACCESS_KEY_ID * ":" * SECRET_ACCESS_KEY
                HOME = homedir()
                region = get_aws_config_region()
                run(pipeline(`echo $passwd_s3fs_contents`, "$HOME/.passwd-s3fs"))
                run(`chmod 600 $HOME/.passwd-s3fs`)
                run(`s3fs $bucket $mount -o url=https://s3.$region.amazonaws.com -o endpoint=$region -o passwd_file=$HOME/.passwd-s3fs`)
            catch e
                @warn "Failed to mount S3 bucket \"$bucket\" at $mount using s3fs with error: $e. You may ensure s3fs is in PATH or mount manually."
                failed_to_use_s3fs = true
            end
        end
    end

    if failed_to_use_s3fs
        # If there is still no mount, return an S3Path
        s3path
    else
        # Return local path to object
        joinpath(mount, key)
    end
end

function with_downloaded_path_for_reading(func::Function, downloaded_path; for_writing=false)
    global tmp_paths

    # There are 3 cases here: `downloaded_path` is an S3Path or a local S3FS
    # path or an http:// file that has been downloaded to tempdir() (i.e., /tmp).

    temp_downloaded_path = if downloaded_path isa S3Path
        temp_downloaded_path = Path(tempname() * splitext(downloaded_path)[2])
        push!(tmp_paths, temp_downloaded_path)
        if !for_writing
            cp(downloaded_path, temp_downloaded_path)
        end
        func(string(temp_downloaded_path))
        if for_writing
            cp(temp_downloaded_path, downloaded_path)
        end
        string(temp_downloaded_path)
    else
        func(string(downloaded_path))
        string(downloaded_path)
    end
end

function cleanup_tmp()
    global tmp_paths
    for tmp_path in tmp_paths
        rm(tmp_path, recursive=true, force=true)
    end
end