# Create a cluster

## Parameters for Cluster Creation

To create a cluster, you must provide the following information:  
* `cluster_id` - name of cluster to create
* `username` - username already registered with Banyan
* `ec2_key_pair` - name of AWS EC2 Key Pair to SSH into the head node of the cluster
* `pcluster_additional_policy` - name of AWS IAM Policy that user created for the cluster
* `num_nodes` - maximu number of nodes in cluster
* `instance_type` - AWS EC2 instance type (one of `t3.large`, `t3.xlarge`, `t3.2xlarge`, `m4.4xlarge`, `m4.10xlarge`, `c5.2xlarge`, `c5.4xlarge`)
* `banyanfile` - Banyanfile describing how to set up cluster and jobs (format described below)
* `s3_read_write_resource` [optional] - ARN of AWS S3 bucket in user account that cluster can access (e.g., to pull source code from or write logs/results back to)


## Format of Banyanfile

The format of a Banyanfile is as follows:
```json
{
    "include": [],
    "require": {
        "language": "jl"|"py",
        "cluster": {
            "commands": ["string",],
            "packages": ["string",],
            "pt_lib_info": "string",
            "pt_lib": "string"
        },
        "job": {
          "code": ["string"]
        }
    }
}
```
* **include** (list)  
List of paths to other Banyanfiles to include, or the actual Banyanfile dictionaries
* **require** (dict)
  * **language** (string)  
  Language used. Currently supporting Julia ("jl")
  * **cluster** (dict)
    * **commands** (list)  
    List of commands to execute on creation of cluster
    * **packages** (list)  
    List of language-dependent packages to install
    * **pt_lib_info** (dict or string)  
    Path to pt_lib_info json file or actual pt_lib_info dict
    * **pt_lib** (string)  
    Optional path to pt_lib
  * **jobs** (dict)  
    * **code** (list)  
    List of lines to code to be executed on creation of a job in this cluster
