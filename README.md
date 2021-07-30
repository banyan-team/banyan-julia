# Banyan Julia

Banyan Julia is an extension to the Julia programming language for that seamlessly scales existing libraries and code to massive data and compute. Banyan allows _partition types_ to be assigned to variables in existing code. Annotated code is automatically offloaded to run in a distributed fashion on managed clusters running in your AWS Virtual Private Cloud. Banyan optimizes code on-the-fly to take advantage of CPU caches and multicores in the clusters where the offloaded code runs.

Software libraries can be annotated with partition types and subsequent use of the annotated functions automatically runs at scale. Currently, we are developing two annotated libraries:

- BanyanArrays.jl
- BanyanDataFrames.jl

Eventually, you will be able to use these libraries as drop-in replacements of the standard library Arrays and the DataFrames.jl library. By changing an import statement, you can run your code as is with Banyan scaling to arbitrary data or compute needs.

Visit [Banyan Computing](https://www.banyancomputing.com/intro/) for full documentation.
