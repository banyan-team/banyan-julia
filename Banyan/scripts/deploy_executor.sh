# #!/bin/bash

# aws s3 cp scripts/executor.jl s3://banyan-executor/
# aws s3 cp scripts/launch_job.sh s3://banyan-executor/
# aws s3 cp scripts/queues.jl s3://banyan-executor/
# aws s3 cp scripts/setup_cluster.sh s3://banyan-executor/
# aws s3 cp res/pt_lib.jl s3://banyan-executor/  # Just a default pt_lib.jl

#!/bin/bash

aws s3 cp scripts/executor.jl s3://banyan-executor/
aws s3 cp scripts/launch_job.sh s3://banyan-executor/
aws s3 cp scripts/queues.jl s3://banyan-executor/
aws s3 cp scripts/setup_cluster.sh s3://banyan-executor/
aws s3 cp res/pt_lib.jl s3://banyan-executor/  # Just a default pt_lib.jl
