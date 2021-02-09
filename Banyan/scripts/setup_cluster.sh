#!/bin/bash
sudo yum update -y &>> setup_log.txt
wget https://julialang-s3.julialang.org/bin/linux/x64/1.5/julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
tar zxvf julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
rm julia-1.5.3-linux-x86_64.tar.gz &>> setup_log.txt
# TODO: The following line should be determined based on packages users want install
julia-1.5.3/bin/julia --project -e "using Pkg; Pkg.add([\"AWSCore\", \"AWSSQS\", \"HTTP\", \"Dates\", \"JSON\", \"MPI\", \"Serialization\"]); ENV[\"JULIA_MPIEXEC\"]=\"srun\"; ENV[\"JULIA_MPI_LIBRARY\"]=\"/opt/amazon/openmpi/lib64/libmpi\"; Pkg.build(\"MPI\"; verbose=true)" &>> setup_log.txt
