# Manage a cluster

## Updating a Cluster

To create a cluster, you must provide the following information.

Required Paramters
* `cluster_name` - name of cluster to update
* `username` - username under which this cluster was created

Optional Parameters
* `additional_policy` - ARN of AWS IAM policy to give additional permissions to the cluster
* `s3_read_write_resource` - ARN of AWS S3 bucket to give cluster read/write permission to
* `num_nodes` - maximum number of nodes for cluster
* `banyanfile` - Banyanfile

Updating a cluster will fail under the following scenarios.
* Cluster is not currently in the `running` state
* Cluster has jobs currently running


## Destroying a Cluster

To destroy a cluster, you must provide the following information:
* `cluster_name` - name of cluster to delete
* `username` - username under which this cluster was created

<!-- Cluster destruction will fail under the following scenarios. To force delete a cluster, even in the following scenarios, set `force=True`.
* Cluster is not currently in the `running` state
* Cluster has jobs currently running -->
