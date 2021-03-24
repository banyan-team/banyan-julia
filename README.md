# banyan-julia

## Testing

Either use your default AWS profile or [create on](https://stackoverflow.com/questions/593334/how-to-use-multiple-aws-accounts-from-the-command-line).

```cmd
cd Banyan

AWS_DEFAULT_PROFILE=banyan-testing
  BANYAN_CLUSTER_NAME=banyancluster
  BANYAN_NWORKERS=2
  BANYAN_USERNAME=pumpkin-at-pie.com
  BANYAN_API_KEY=7FBKWAv3ld0eOfghSwhX_g
  julia --project=. -e "using Pkg; Pkg.test(\"Banyan\", test_args=[\"scholes\"])"
```

In a single line:

```cmd
AWS_DEFAULT_PROFILE=banyan-testing BANYAN_CLUSTER_NAME=banyancluster BANYAN_NWORKERS=2 BANYAN_USERNAME=pumpkin-at-pie.com BANYAN_API_KEY=7FBKWAv3ld0eOfghSwhX_g julia --project=. -e "using Pkg; Pkg.test(\"Banyan\", test_args=[\"scholes\"])"
```

### Configure AWS Credentials

Since AWS Parallel Cluster is launched in BanyanTestUser account, you will need
to configure AWS credentials for this account. Initially, you will need to
generate these credentials.
- Login to the AWS dashboard and go to the IAM dashboard
- Select `Users` on the left bar, and then select `Add user`
- Enter a user name and select Access type `Programmatic access`
- Select `Attach existing policies directly` and select `Administrator Access`
- Click through the remaining default settings
- Save the AWS_ACCESS_KEY and AWS_SECRET_KEY securely

To configure AWS credentials as environment variables:
```cmd
> aws configure
  AWS Access Key ID []: AWS_ACCESS_KEY
  AWS Secret Access Key []]: AWS_SECRET_KEY
  Default region name []: us-west-2
```

To SSH into pcluster:
```cmd
> pcluster ssh CLUSTER_ID -i /PATH/TO/SSH_KEY_PAIR
```

For the current running cluster,
```cmd
> pcluster ssh banyancluster -i /PATH/TO/EC2ConnectKeyPairTest
```
