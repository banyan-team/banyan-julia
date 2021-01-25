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
			echo "bash run_executor.sh --environment-id <environment ID> --size-n <# of nodes> --size-ct <# of CPUs>"
			echo
			echo "If you would like to use the default (4 nodes and 1 CPU per node, run the following:"
			echo "bash run_executor.sh --environment_id <environment_id> --default"
			exit
			;;
		--environment-id)
			environment_id="$2"
			;;
		--size-n)
			size_n="$2"
			;;
		--size-ct)
			size_ct="$2"
			;;
		--default)
			size_n=4
			size_ct=1
			;;
	esac
	shift
done

# Submit Slurm job
# TODO: Export a JULIA_NUM_THREADS environment variable to each node
# TODO: Determine if we can relax the constraint on # of nodes and just ensure
# we have size_n tasks
touch srun_command_${environment_id}.sh
echo '#!/bin/bash' > srun_command_${environment_id}.sh
echo '#SBATCH --ntasks='${size_n} >> srun_command_${environment_id}.sh
echo '#SBATCH --cpus-per-task='${size_ct} >> srun_command_${environment_id}.sh
# echo 'export JULIA_NUM_THREADS='${size_ct} >> srun_command_${environment_id}.sh
echo -n 'srun ' >> srun_command_${environment_id}.sh
echo -n '--mpi=pmix ' >> srun_command_${environment_id}.sh
echo -n '--ntasks='${size_n}' --cpus-per-task='${size_ct}' ' >> srun_command_${environment_id}.sh
echo -n 'julia-1.5.3/bin/julia --threads '${size_ct}' executor.jl' ${environment_id} >> srun_command_${environment_id}.sh
# TODO: Use julia --threads size_ct if above doesn't work
sbatch --job-name ${environment_id} srun_command_${environment_id}.sh
rm srun_command_${environment_id}.sh