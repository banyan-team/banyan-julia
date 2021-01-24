import sys
import json
import subprocess

import boto3
from boto3.dynamodb.types import TypeSerializer

s3 = boto3.resource('s3')

s3_client = boto3.client('s3')
s3_client.upload_file("res/pt_lib.jl", "banyan-executor", "pt_lib.jl")
# TODO: Determine whether an && cd /home/ec2-executor is needed here
subprocess.run(
    f"""pcluster ssh {sys.argv[1]} -i {sys.argv[2]}
    sudo yum update -y &&
    aws s3 cp s3://banyan-executor /home/ec2-user --recursive""".split()
)

dynamodb = boto3.resource('dynamodb')

clusters = dynamodb.Table('Clusters')
serializer = TypeSerializer()
with open('res/pt_lib_info.json') as pt_lib_info_file:
    pt_lib_info_json = json.load(pt_lib_info_file)
    clusters.update_item(
        Key={
            'cluster_id': sys.argv[1],
        },
        UpdateExpression='SET pt_lib_info = :pt_lib_info',
        ExpressionAttributeValues={
            ':pt_lib_info': {
                k: serializer.serialize(v) for k, v in pt_lib_info_json.items()
            }
        }
    )