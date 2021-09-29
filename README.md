# Banyan Julia

Banyan Julia is an extension to the Julia programming language for that seamlessly scales existing libraries and code to massive data and compute. Banyan allows _partition types_ to be assigned to variables in existing code. Annotated code is automatically offloaded to run in a distributed fashion on managed clusters running in your AWS Virtual Private Cloud. Banyan optimizes code on-the-fly to take advantage of CPU caches and multicores in the clusters where the offloaded code runs.

Software libraries can be annotated with partition types and subsequent use of the annotated functions automatically runs at scale. Currently, we are developing two annotated libraries:

- BanyanArrays.jl
- BanyanDataFrames.jl

Eventually, you will be able to use these libraries as drop-in replacements of the standard library Arrays and the DataFrames.jl library. By changing an import statement, you can run your code as is with Banyan scaling to arbitrary data or compute needs.

Visit [Banyan Computing](https://www.banyancomputing.com/resources/) for full documentation.

## Contributing

Please create branches named according the the author name and the feature name
like `{author-name}/{feature-name}`. For example: `caleb/add-tests-for-hdf5`.
Then, submit a pull request on GitHub to merge your branch into the branch with
the latest version number.

When pulling/pushing code, you may need to add the appropriate SSH key. Look
up GitHub documentation for how to generate an SSH key, then make sure to add
it. You may need to do this repeatedly if you have multiple SSH keys for
different GitHub accounts. For example, on Windows you may need to:

```
eval `ssh-agent`
ssh-add -D
ssh-add /c/Users/Claris/.ssh/id_rsa_clarisw
git remote set-url origin git@github.com:banyan-team/banyan-website.git
```

## Testing

To see an example of how to add tests, see `BanyanArrays/test/runtests.jl` and `BanyanArrays/test/hdf5.jl`.

To run tests, ensure that you have a Banyan account connected to an AWS account.
Then, `cd` into the directory with the Banyan Julia project you want to run
tests for (e.g., `Banyan` for Banyan.jl or `BanyanDataFrames` for
BanyanDataFrames.jl) and run `julia --project=. -e "using Pkg; Pkg.test()"`.
To filter and run a subset of test sets (where each test set is defined with
`@testset`) with names matching a given pattern, run
`julia --project=. -e "using Pkg; Pkg.test(test_args=[\"{pattern 1}\", \"{pattern 2}\"])"` where
the pattern could be, for example, `Sampl(.*)parquet` (a regular expression)
or `Sample collection`.

You must then specify the cluster name with the `BANYAN_CLUSTER_NAME`
environment variable. You must also specify the relevant `BANYAN_*`
and `AWS_*` environment variables to provide credentials. AWS
credentials are specified in the same way as they would be if using
the AWS CLI (either use environment variables or use the relevant
AWS configuration files) and the Banyan environment variables
are saved in `banyanconfig.toml` so you don't need to specify it
every time.

## Development

Make sure to use the `] dev ...` command or `Pkg.dev(...)` to ensure that when you
are using BanyanArrays.jl or BanyanDataFrames.jl you are using the local version.