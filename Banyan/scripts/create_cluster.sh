if [ $# -eq 0 ]; then
	echo "Please run this script with ./deploy_pt_lib.sh <cluster name> <config file> <SSH key pair>";
	exit 0
fi

# Create cluster
pcluster create -c $2 $1

# Update DynamoDB table
aws dynamodb put-item \
    --table-name Clusters \
    --item '{ \
        "cluster_id": "$1", \
        "pt_lib_info": {}, \
 	}'