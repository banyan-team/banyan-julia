# Run this script with cluster name
if [ $# -eq 0 ]; then
	echo "Please run this script with ./deploy_pt_lib.sh <cluster name> <SSH key pair>";
	exit 0
fi


# Upload pt_lib.jl to S3, and download onto cluster

aws s3 cp res/pt_lib.jl s3://banyan-executor/

pcluster ssh $1 -i $2 /bin/bash << EOF

# Update
sudo yum update -y

# Pull code from S3
aws s3 cp s3://banyan-executor /home/ec2-user --recursive
cd /home/ec2-user

EOF


# Upload pt_lib_info.json to Clusters DynamoDB Table

aws dynamodb update-item \
	--table-name Clusters  \
	--key '{"cluster_id": {"S": "'"$1"'"}}' \
	--update-expression 'SET pt_lib_info = :pt_lib_info' \
	--expression-attribute-values file://res/pt_lib_info.json
