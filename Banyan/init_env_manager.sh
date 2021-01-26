#!/bin/bash

# Update
sudo yum update -y

# Install Julia
wget https://julialang-s3.julialang.org/bin/linux/x64/1.5/julia-1.5.3-linux-x86_64.tar.gz
tar zxvf julia-1.5.3-linux-x86_64.tar.gz

# Initialize Julia to use the OpenMPI installation already provided by
# 	AWS ParallelCluster launched with Slurm.
julia-1.5.3/bin/julia --project -e 'using Pkg; Pkg.add("AWSCore"); Pkg.add("AWSSQS"); Pkg.add("HTTP"); Pkg.add("JSON"); Pkg.add("MPI"); Pkg.add("Serialization")'
julia-1.5.3/bin/julia --project -e 'ENV["JULIA_MPIEXEC"]="srun"; ENV["JULIA_MPI_LIBRARY"]="/opt/amazon/openmpi/lib64/libmpi"'
julia-1.5.3/bin/julia --project -e 'using Pkg; Pkg.build("MPI"; verbose=true)'

# Pull code from S3
aws s3 cp s3://banyan-executor /home/ec2-user --recursive
cd /home/ec2-user