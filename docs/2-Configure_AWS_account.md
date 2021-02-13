# Configure AWS account

To use Banyan, you must grant Banyan permission to create and manage compute resources. **Banyan can use either a cross-account role or access keys???or just cross-account role...**. For either method, you must configure settings in the AWS Management Console and the Banyan Dashboard. This document describes both methods. While both methods are supported, we recommend that you use a cross-account role.

## Use a cross-account role

### Step 1: Configure Banyan to use a cross-account role
1. Sign into your Banyan account and go to the Banyan Dashboard.
2. ...
3. Copy the external ID for later use in Step 2.

### Step 2: Configure AWS IAM role settings

1. Sign into your AWS account and go to the IAM service in the AWS Management Console.
2. Click the **Policies** tab in the sidebar.
3. Click **Create Policy** to create a new IAM policy.
    a. Click the **JSON** tab.  
    b. Paste the following policy into the editor, replacing <account-id> with your account id.  
    ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "ec2:*"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "EC2"
                },
                {
                    "Action": [
                        "autoscaling:DescribeAutoScalingGroups",
                        "autoscaling:DescribeAutoScalingInstances",
                        "autoscaling:CreateAutoScalingGroup",
                        "autoscaling:PutNotificationConfiguration",
                        "autoscaling:UpdateAutoScalingGroup",
                        "autoscaling:PutScalingPolicy",
                        "autoscaling:DescribeScalingActivities",
                        "autoscaling:DeleteAutoScalingGroup",
                        "autoscaling:DeletePolicy",
                        "autoscaling:DisableMetricsCollection",
                        "autoscaling:EnableMetricsCollection"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "AutoScaling"
                },
                {
                    "Action": [
                        "dynamodb:DescribeTable",
                        "dynamodb:ListTagsOfResource",
                        "dynamodb:CreateTable",
                        "dynamodb:DeleteTable",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "dynamodb:TagResource"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "DynamoDB"
                },
                {
                    "Action": [
                        "route53:ChangeResourceRecordSets",
                        "route53:ChangeTagsForResource",
                        "route53:CreateHostedZone",
                        "route53:DeleteHostedZone",
                        "route53:GetChange",
                        "route53:GetHostedZone",
                        "route53:ListResourceRecordSets",
                        "route53:ListQueryLoggingConfigs"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "Route53HostedZones"
                },
                {
                    "Action": [
                        "sqs:*"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SQS"
                },
                {
                    "Action": [
                        "sns:*"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SNS"
                },
                {
                    "Action": [
                        "cloudformation:DescribeStackEvents",
                        "cloudformation:DescribeStackResource",
                        "cloudformation:DescribeStackResources",
                        "cloudformation:DescribeStacks",
                        "cloudformation:ListStacks",
                        "cloudformation:GetTemplate",
                        "cloudformation:CreateStack",
                        "cloudformation:DeleteStack",
                        "cloudformation:UpdateStack"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "CloudFormation"
                },
                {
                    "Action": [
                        "s3:*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::parallelcluster-*"
                    ],
                    "Effect": "Allow",
                    "Sid": "S3ResourcesBucket"
                },
                {
                    "Action": [
                        "s3:Get*",
                        "s3:List*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::us-west-2-aws-parallelcluster*"
                    ],
                    "Effect": "Allow",
                    "Sid": "S3ParallelClusterReadOnly"
                },
                {
                    "Action": [
                        "s3:DeleteBucket",
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion"
                    ],
                    "Resource": [
                        "arn:aws:s3:::parallelcluster-*"
                    ],
                    "Effect": "Allow",
                    "Sid": "S3Delete"
                },
                {
                    "Action": [
                        "iam:PassRole",
                        "iam:CreateRole",
                        "iam:CreateServiceLinkedRole",
                        "iam:DeleteRole",
                        "iam:GetRole",
                        "iam:TagRole",
                        "iam:SimulatePrincipalPolicy"
                    ],
                    "Resource": [
                        "arn:aws:iam::338603620317:role/<PARALLELCLUSTER EC2 ROLE NAME>",
                        "arn:aws:iam::338603620317:role/parallelcluster-*",
                        "arn:aws:iam::338603620317:role/aws-service-role/*"
                    ],
                    "Effect": "Allow",
                    "Sid": "IAMModify"
                },
                {
                    "Action": [
                        "iam:CreateInstanceProfile",
                        "iam:DeleteInstanceProfile"
                    ],
                    "Resource": "arn:aws:iam::338603620317:instance-profile/*",
                    "Effect": "Allow",
                    "Sid": "IAMCreateInstanceProfile"
                },
                {
                    "Action": [
                        "iam:AddRoleToInstanceProfile",
                        "iam:RemoveRoleFromInstanceProfile",
                        "iam:GetRolePolicy",
                        "iam:GetPolicy",
                        "iam:AttachRolePolicy",
                        "iam:DetachRolePolicy",
                        "iam:PutRolePolicy",
                        "iam:DeleteRolePolicy"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "IAMInstanceProfile"
                },
                {
                    "Action": [
                        "elasticfilesystem:DescribeMountTargets",
                        "elasticfilesystem:DescribeMountTargetSecurityGroups",
                        "ec2:DescribeNetworkInterfaceAttribute"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "EFSDescribe"
                },
                {
                    "Action": [
                        "ssm:GetParametersByPath"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SSMDescribe"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "ssm:SendCommand"
                    ],
                    "Resource": [
                        "arn:aws:ec2:us-west-2:338603620317:instance/*",
                        "arn:aws:ssm:us-west-2::document/AWS-RunShellScript",
                        "arn:aws:s3:::pcluster-data/ssm"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "ssm:GetCommandInvocation"
                    ],
                    "Resource": [
                        "arn:aws:ssm:us-west-2:338603620317:*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::pcluster-data",
                        "arn:aws:s3:::pcluster-data/*"
                    ]
                },
                {
                    "Action": [
                        "fsx:*"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "FSx"
                },
                {
                    "Action": [
                        "elasticfilesystem:*"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "EFS"
                },
                {
                    "Action": [
                        "logs:DeleteLogGroup",
                        "logs:PutRetentionPolicy",
                        "logs:DescribeLogGroups",
                        "logs:CreateLogGroup"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "CloudWatchLogs"
                },
                {
                    "Action": [
                        "lambda:CreateFunction",
                        "lambda:DeleteFunction",
                        "lambda:GetFunctionConfiguration",
                        "lambda:GetFunction",
                        "lambda:InvokeFunction",
                        "lambda:AddPermission",
                        "lambda:RemovePermission"
                    ],
                    "Resource": [
                        "arn:aws:lambda:us-west-2:338603620317:function:parallelcluster-*",
                        "arn:aws:lambda:us-west-2:338603620317:function:pcluster-*"
                    ],
                    "Effect": "Allow",
                    "Sid": "Lambda"
                },
                {
                    "Sid": "CloudWatch",
                    "Effect": "Allow",
                    "Action": [
                    "cloudwatch:PutDashboard",
                    "cloudwatch:ListDashboards",
                    "cloudwatch:DeleteDashboards",
                    "cloudwatch:GetDashboard"
                    ],
                    "Resource": "*"
                }
            ]
        }
    ```
    c. Click **Review Policy**.  
    d. In the **Name** field, enter `BanyanAccessPolicy` or a different name for the IAM policy.  
    e. Click **Create Policy**.
3. Return to the IAM service in the AWS Management Console.
4. Click on the **Roles** tab in the sidebar.
5. Select **Create Role** to create a new IAM role.
    a. Under **Select type of trusted entity**, select **Another AWS account**.  
    b. In the **Account ID** field, enter the Banyan account ID `<account-id>`.  
    c. Select **Require external ID**.  
    d. In the **External ID** field, enter the external id you acquired from the Banyan Dashboard in Step 1.  
    e. Search for `BanyanAccessPolicy` or the name you gave the policy you just created, and select it.  
    f. Click **Next: Tags**.  
    g. Click **Next: Review**.  
    h. In the **Name** field, enter `BanyanAccessRole` or a different name for the IAM role.  
6. Click on the role you just created and copy the **Role ARN**.

### Step 3: Configure the cross-account role in your Banyan account

1. Enter in role ARN





## Use access keys