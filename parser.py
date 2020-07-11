import json
import boto3
import mypy_boto3_s3 as s3
import glob
import gzip

def extractObject(json_object, search_object):

    get_object_list = []
    for log in json_object['Records']:
        if search_object in log.values():
            if 'errorCode' in log.keys():
                get_object_list.append(log)
    if len(get_object_list) == 0:
        return False, get_object_list
    else:
        return True, get_object_list

def getLogs(bucket_name, root_key):

    client: s3.S3Client = boto3.client('s3')
    # client = boto3.client('s3')
    logs = []
    
    for element in client.list_objects_v2(Bucket=bucket_name)['Contents']:
        if root_key in element['Key']:
            if root_key != element['Key']:
                response = client.get_object(Bucket=bucket_name,Key=element['Key'])
                logs.append(response)
    
    return logs

def handler(event, context):
    
    message = event['Records']
    print(message)

if __name__ == '__main__':

    get_object_list = []
    logs_from_s3 = []
    bucket_name = '7654-2707-2911-bucket-logs' #here is the bucket used for the trail that is configured on the object level logging
    bucket_root_key = 'AWSLogs/765427072911/CloudTrail/eu-west-1/2020/07/10/' #this is the "path" to the date of the event inside the bucket
    event = 'GetObject'

    print("Fetching logs from:",bucket_name,bucket_root_key)
    logs_from_s3 = getLogs(bucket_name, bucket_root_key)
    
    for log in logs_from_s3:
        with gzip.open(log['Body']) as f:
            data = json.load(f)
            result, object_list = extractObject(json_object=data,search_object=event)
            if result:
                get_object_list.append(object_list)
    
    print(get_object_list)

