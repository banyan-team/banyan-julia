#!/bin/bash

# Usage: Run this script from the environment manager to submit the Slurm job.

# Get user-provided information
if [ $# -eq 0 ]; then
	echo "To see options, run init_executor.sh --help";
	exit 0 
fi
while [ $# -gt 0 ]; do
	case "$1" in
		--help)
			echo "Please run this script as follows:"
			echo "bash run_executor.sh --job-id <job ID> --num-workers <# of workers>"
			echo
			echo "If you would like to use the default (4 nodes and 1 CPU per node, run the following:"
			echo "bash run_executor.sh --job_id <job_id> --default"
			exit
			;;
		--job-id)
			job_id="$2"
			;;
		--num-workers)
			num_workers="$2"
			;;
		--region)
			region="$2"
			;;
		--default)
			num_workers=8
			;;
	esac
	shift
done

# Submit Slurm job
touch srun_command_${job_id}.sh
echo '#!/bin/bash' > srun_command_${job_id}.sh
echo '#SBATCH --ntasks='${num_workers} >> srun_command_${job_id}.sh
echo '#SBATCH --cpus-per-task=1' >> srun_command_${job_id}.sh  # TODO: Is this right?
echo -n 'srun ' >> srun_command_${job_id}.sh
echo -n '--mpi=pmix ' >> srun_command_${job_id}.sh
echo -n '--ntasks='${num_workers}' --cpus-per-task=1 ' >> srun_command_${job_id}.sh
echo -n 'julia-1.5.3/bin/julia executor.jl' ${job_id} >> srun_command_${job_id}.sh

# jq --arg output "$(<slurm-.out.txt)" -n '{"Output":{"DataType":"String","StringValue":$output}}' > srun_command_'${job_id}'_output.json
# touch srun_command_${job_id}_return.sh
# echo 'jq --arg output \"$(<slurm-$1.out.txt)\" -n \'{\"message_type\": $2, \"output\": \$output}\' > srun_command_\'${job_id}\'_output.json' > srun_command_${job_id}_return.sh

touch srun_command_${job_id}_return.sh
echo '#!/bin/bash' > srun_command_${job_id}_return.sh
echo 'touch srun_command_'${job_id}'_output.txt' >> srun_command_${job_id}_return.sh
echo 'echo JOB_FAILURE > srun_command_'${job_id}'_output.txt' >> srun_command_${job_id}_return.sh
echo 'cat banyan-log-for-job-'${job_id}' >> srun_command_'${job_id}'_output.txt' >> srun_command_${job_id}_return.sh
echo 'echo -n > banyan-log-for-job-'${job_id} >> srun_command_${job_id}_return.sh
echo 'OUTPUT=$(<srun_command_'${job_id}'_output.txt)' >> srun_command_${job_id}_return.sh
echo 'QUEUE_URL=$(aws sqs get-queue-url --queue-name=banyan_'${job_id}'_gather.fifo --region='${region}' --output=text)' >> srun_command_${job_id}_return.sh
#echo -n 'srun ' >> srun_command_${job_id}_return.sh
#echo -n 'aws sqs send-message --queue-url=$QUEUE_URL --message-body="$OUTPUT" --message-group-id=1 --message-deduplication-id=JOB_FAILURE_TOKEN --region='${region} >> srun_command_${job_id}_return.sh
echo 'aws sqs send-message --queue-url=$QUEUE_URL --message-body="$OUTPUT" --message-group-id=1 --message-deduplication-id=JOB_FAILURE_TOKEN --region='${region} >> srun_command_${job_id}_return.sh
#echo 'rm ' >> srun_command_${job_id}_return.sh

# TODO: Make this more robust by handling the case where the cluster name is provided and the output even with --parsable is semicolon-seperated
ENV["JULIA_MPIEXEC"]="srun" && ENV["JULIA_MPI_LIBRARY"]="/opt/amazon/openmpi/lib64/libmpi" && LAUNCHED_JOB_ID=$(sbatch --job-name=${job_id} --output=banyan-log-for-job-${job_id} --parsable srun_command_${job_id}.sh)
sbatch --job-name=${job_id}-on-return --dependency=afternotok:$LAUNCHED_JOB_ID srun_command_${job_id}_return.sh
rm -f srun_command_${job_id}.sh
rm -f srun_command_${job_id}_return.sh
rm -rf job_${job_id}*