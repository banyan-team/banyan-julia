#!/bin/bash

if [ $# -eq 0 ]; then
	echo "Please run this script with ./deploy_pt_lib.sh <cluster name> <SSH key pair>";
	exit 0
fi

pcluster ssh $1 -i $2 /bin/bash << EOF

# Pull code from S3
aws s3 cp s3://banyan-executor /home/ec2-user --recursive
cd /home/ec2-user

EOF
