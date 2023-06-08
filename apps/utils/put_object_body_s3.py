import boto3

def assume_role(role_arn):
    sts = boto3.client('sts')
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName='AssumedRoleSession'
    )
    credentials = response['Credentials']
    return credentials

def list_objects(bucket_name, prefix, s3_client):
    #credentials = assume_role(role_arn)
    #s3 = boto3.client('s3',
    #                  aws_access_key_id=credentials['AccessKeyId'],
    #                  aws_secret_access_key=credentials['SecretAccessKey'],
    #                  aws_session_token=credentials['SessionToken'])
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    objects = []
    for page in page_iterator:
        if 'Contents' in page:
            objects.extend(page['Contents'])

    return objects

def put_object(bucket_name, key, s3_client, body):
    #credentials = assume_role(role_arn)
    #s3 = boto3.client('s3',
    #                  aws_access_key_id=credentials['AccessKeyId'],
    #                  aws_secret_access_key=credentials['SecretAccessKey'],
    #                  aws_session_token=credentials['SessionToken'])
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

# Usage example
bucket_name = 'your-bucket-name'
prefix = 'your-object-prefix'
role_arn = 'arn:aws:iam::123456789012:role/YourRoleName'

s3_client = boto3.client('s3',
aws_access_key_id='',
aws_secret_access_key='',
aws_session_token='')

result = list_objects(bucket_name, prefix, s3_client)
for obj in result:
    key = obj['Key']
    put_object(bucket_name, key, s3_client, body)
    print(f"Put new object in: {key}")