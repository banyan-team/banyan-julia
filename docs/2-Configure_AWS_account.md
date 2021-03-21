# Configure AWS account

To use Banyan, you must first grant Banyan sufficient permissions to create an manage compute resources in your account. Banyan uses a cross-account role for this. You must configure settings both in the AWS Management Console and in the Banyan Dashboard. Once you have configured your account, you cannot associate your Banyan account with a different AWS account.

## Step 1: Configure Banyan to use a cross-account role
1. Sign into your Banyan account and go to the Banyan Dashboard.
2. Click on **Account**
3. Copy the External ID for later use in Step 2.

### Step 2: Configure AWS IAM role settings

1. Sign into your AWS account and go to the IAM service in the AWS Management Console.
2. Click the **Policies** tab in the sidebar.
3. Click **Create Policy** to create a new IAM policy.  
    a. Click the **JSON** tab.  
    b. Paste the following policy into the editor, replacing \<account-id\> with your account id.  
    <details>
    <summary>Expand to view policy.</summary>
    
    ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "ec2:DescribeKeyPairs",
                        "ec2:DescribeRegions",
                        "ec2:DescribeVpcs",
                        "ec2:DescribeSubnets",
                        "ec2:DescribeSecurityGroups",
                        "ec2:DescribePlacementGroups",
                        "ec2:DescribeImages",
                        "ec2:DescribeInstances",
                        "ec2:DescribeInstanceStatus",
                        "ec2:DescribeInstanceTypes",
                        "ec2:DescribeInstanceTypeOfferings",
                        "ec2:DescribeSnapshots",
                        "ec2:DescribeVolumes",
                        "ec2:DescribeVpcAttribute",
                        "ec2:DescribeAddresses",
                        "ec2:CreateTags",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DescribeAvailabilityZones"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "EC2Describe"
                },
                {
                    "Action": [
                        "ec2:CreateVpc",
                        "ec2:ModifyVpcAttribute",
                        "ec2:DescribeNatGateways",
                        "ec2:CreateNatGateway",
                        "ec2:DescribeInternetGateways",
                        "ec2:CreateInternetGateway",
                        "ec2:AttachInternetGateway",
                        "ec2:DescribeRouteTables",
                        "ec2:CreateRoute",
                        "ec2:CreateRouteTable",
                        "ec2:AssociateRouteTable",
                        "ec2:CreateSubnet",
                        "ec2:ModifySubnetAttribute"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "NetworkingEasyConfig"
                },
                {
                    "Action": [
                        "ec2:CreateVolume",
                        "ec2:RunInstances",
                        "ec2:AllocateAddress",
                        "ec2:AssociateAddress",
                        "ec2:AttachNetworkInterface",
                        "ec2:AuthorizeSecurityGroupEgress",
                        "ec2:AuthorizeSecurityGroupIngress",
                        "ec2:CreateNetworkInterface",
                        "ec2:CreateSecurityGroup",
                        "ec2:ModifyVolumeAttribute",
                        "ec2:ModifyNetworkInterfaceAttribute",
                        "ec2:DeleteNetworkInterface",
                        "ec2:DeleteVolume",
                        "ec2:TerminateInstances",
                        "ec2:DeleteSecurityGroup",
                        "ec2:DisassociateAddress",
                        "ec2:RevokeSecurityGroupIngress",
                        "ec2:RevokeSecurityGroupEgress",
                        "ec2:ReleaseAddress",
                        "ec2:CreatePlacementGroup",
                        "ec2:DeletePlacementGroup"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "EC2Modify"
                },
                {
                    "Action": [
                        "autoscaling:DescribeAutoScalingGroups",
                        "autoscaling:DescribeAutoScalingInstances"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "AutoScalingDescribe"
                },
                {
                    "Action": [
                        "autoscaling:CreateAutoScalingGroup",
                        "ec2:CreateLaunchTemplate",
                        "ec2:CreateLaunchTemplateVersion",
                        "ec2:ModifyLaunchTemplate",
                        "ec2:DeleteLaunchTemplate",
                        "ec2:DescribeLaunchTemplates",
                        "ec2:DescribeLaunchTemplateVersions",
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
                    "Sid": "AutoScalingModify"
                },
                {
                    "Action": [
                        "dynamodb:DescribeTable",
                        "dynamodb:ListTagsOfResource"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "DynamoDBDescribe"
                },
                {
                    "Action": [
                        "dynamodb:CreateTable",
                        "dynamodb:DeleteTable",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "dynamodb:TagResource"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "DynamoDBModify"
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
                        "sqs:GetQueueAttributes"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SQSDescribe"
                },
                {
                    "Action": [
                        "sqs:CreateQueue",
                        "sqs:DeleteMessage",
                        "sqs:DeleteQueue",
                        "sqs:GetQueueUrl",
                        "sqs:ListQueues",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                        "sqs:SetQueueAttributes",
                        "sqs:TagQueue"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SQSModify"
                },
                {
                    "Action": [
                        "sns:ListTopics",
                        "sns:GetTopicAttributes"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SNSDescribe"
                },
                {
                    "Action": [
                        "sns:CreateTopic",
                        "sns:Subscribe",
                        "sns:Unsubscribe",
                        "sns:DeleteTopic"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "SNSModify"
                },
                {
                    "Action": [
                        "cloudformation:DescribeStackEvents",
                        "cloudformation:DescribeStackResource",
                        "cloudformation:DescribeStackResources",
                        "cloudformation:DescribeStacks",
                        "cloudformation:ListStacks",
                        "cloudformation:GetTemplate"
                    ],
                    "Resource": "*",
                    "Effect": "Allow",
                    "Sid": "CloudFormationDescribe"
                },
                {
                    "Action": [
                        "cloudformation:CreateStack",
                        "cloudformation:DeleteStack",
                        "cloudformation:UpdateStack"
                    ],
                    "Effect": "Allow",
                    "Resource": "*",
                    "Sid": "CloudFormationModify"
                },
                {
                    "Action": [
                        "s3:*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::<RESOURCES S3 BUCKET>"
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
                        "arn:aws:s3:::<REGION>-aws-parallelcluster*"
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
                        "arn:aws:s3:::<RESOURCES S3 BUCKET>"
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
                        "arn:aws:iam::<AWS ACCOUNT ID>:role/<PARALLELCLUSTER EC2 ROLE NAME>",
                        "arn:aws:iam::<AWS ACCOUNT ID>:role/parallelcluster-*",
                        "arn:aws:iam::<AWS ACCOUNT ID>:role/aws-service-role/*"
                    ],
                    "Effect": "Allow",
                    "Sid": "IAMModify"
                },
                {
                    "Action": [
                        "iam:CreateInstanceProfile",
                        "iam:DeleteInstanceProfile"
                    ],
                    "Resource": "arn:aws:iam::<AWS ACCOUNT ID>:instance-profile/*",
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
                        "arn:aws:lambda:<REGION>:<AWS ACCOUNT ID>:function:parallelcluster-*",
                        "arn:aws:lambda:<REGION>:<AWS ACCOUNT ID>:function:pcluster-*"
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
                },
                {
                    "Sid": "BanyanParallelCluster",
                    "Effect": "Allow",
                    "Action": [


                        "autoscaling:DescribeAutoScalingGroups",
                        "autoscaling:DescribeTags",
                        "autoscaling:SetDesiredCapacity",
                        "autoscaling:SetInstanceHealth",
                        "autoscaling:TerminateInstanceInAutoScalingGroup",
                        "autoScaling:UpdateAutoScalingGroup",
                        "cloudformation:DescribeStacks",
                        "dynamodb:DescribeTable",
                        "dynamodb:DeleteItem",
                        "dynamodb:GetItem",
                        "dynamodb:ListTables",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "ec2:AttachVolume",
                        "ec2:DescribeInstanceAttribute",
                        "ec2:DescribeInstances",
                        "ec2:DescribeInstanceStatus",
                        "ec2:DescribeRegions",
                        "ec2:DescribeVolumes",
                        "ec2messages:AcknowledgeMessage",
                        "ec2messages:DeleteMessage",
                        "ec2messages:FailMessage",
                        "ec2messages:GetEndpoint",
                        "ec2messages:GetMessages",
                        "ec2messages:SendReply",
                        "s3:*",
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                        "ssm:ListInstanceAssociations",
                        "ssm:PutComplianceItems",
                        "ssm:DescribeAssociation",
                        "ssm:DescribeDocument",
                        "ssm:GetDocument",
                        "ssm:ListAssociations",
                        "ssm:GetDeployablePatchSnapshotForInstance",
                        "ssm:GetManifest",
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:UpdateAssociationStatus",
                        "ssm:UpdateInstanceAssociationStatus",
                        "ssm:UpdateInstanceInformation",
                        "ssm:PutConfigurePackageResult",
                        "ssm:PutInventory",
                        "ssmmessages:CreateControlChannel",
                        "ssmmessages:CreateDataChannel",
                        "ssmmessages:OpenControlChannel",
                        "ssmmessages:OpenDataChannel",
                    ],
                    "Resource": "*"
                }
            ]
        }
    ```
    </details>  

    c. Click **Review Policy**.  
    d. In the **Name** field, enter `BanyanAccessPolicy` or a different name for the IAM policy.  
    e. Click **Create Policy**.  
3. Return to the IAM service in the AWS Management Console.
4. Click on the **Roles** tab in the sidebar.
5. Select **Create Role** to create a new IAM role.
    a. Under **Select type of trusted entity**, select **Another AWS account**.  
    b. In the **Account ID** field, enter the Banyan account ID `<account-id>`TODO:put our account id.  
    c. Select **Require external ID**.  
    d. In the **External ID** field, enter the external id you acquired from the Banyan Dashboard in Step 1.  
    e. Search for `BanyanAccessPolicy` or the name you gave the policy you just created, and select it.  
    f. Click **Next: Tags**.  
    g. Click **Next: Review**.  
    h. In the **Name** field, enter `BanyanAccessRole` or a different name for the IAM role.  
6. Click on the role you just created and copy the **Role ARN**.

### Step 3: Configure the cross-account role in your Banyan account

1. Return to the form from Step 1
2. Enter in role ARN you copied from Step 2
3. Enter in your default aws-region
4. Click submit


