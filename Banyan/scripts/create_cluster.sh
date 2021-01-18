if [ $# -eq 0 ]; then
	echo "Please run this script with ./deploy_pt_lib.sh <cluster name> <config file> <SSH key pair>";
	exit 0
fi

# TODO: Specify language

cluster_id=$1

# Create cluster
pcluster create -c $2 $1

pcluster ssh $1 -i $3 /bin/bash << EOF

# Update
sudo yum update -y

# Install Julia
wget https://julialang-s3.julialang.org/bin/linux/x64/1.5/julia-1.5.3-linux-x86_64.tar.gz
tar zxvf julia-1.5.3-linux-x86_64.tar.gz
rm julia-1.5.3-linux-x86_64.tar.gz

# Initialize Julia to use the OpenMPI installation already provided by
# 	AWS ParallelCluster launched with Slurm.
julia-1.5.3/bin/julia --project -e 'using Pkg; Pkg.add("AWSCore"); Pkg.add("AWSSQS"); Pkg.add("Dates"); Pkg.add("HTTP"); Pkg.add("JSON"); Pkg.add("MPI"); Pkg.add("Serialization")'
julia-1.5.3/bin/julia --project -e 'using Pkg; ENV["JULIA_MPIEXEC"]="srun"; ENV["JULIA_MPI_LIBRARY"]="/opt/amazon/openmpi/lib64/libmpi; Pkg.build("MPI", verbose=true)'

# Pull code from S3
aws s3 cp s3://banyan-executor /home/ec2-user --recursive
cd /home/ec2-user

EOF

# Update DynamoDB table
aws dynamodb put-item \
    --table-name Clusters \
    --item '{"cluster_id": {"S": "'$cluster_id'"}, "language": {"S": "jl"}, "pt_lib_info": {"M": {}}}'
