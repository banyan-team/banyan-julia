#!/bin/bash

if [ $# -eq 0 ]; then
	echo "Please run this script with bash create_environment.sh <cluster name> <SSH key pair> <job ID> <# of workers>";
	exit 0
fi

# SSH into the pcluster
pcluster ssh $1 -i $2 /bin/bash << EOF

bash launch_job.sh --environment-id $3 --num-workers $4

EOF