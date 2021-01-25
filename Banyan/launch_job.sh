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
sbatch --job-name ${job_id} srun_command_${job_id}.sh
rm srun_command_${job_id}.sh
