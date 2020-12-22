#!/bin/bash

if [ $# -eq 0 ]; then
	echo "Please run this script with destroy_environment.sh <pcluster name> <SSH_key_pair> <job_id>";
	exit 0 
fi

# SSH into the pcluster
pcluster ssh $1 -i $2 /bin/bash << EOF

scancel -n $3

EOF