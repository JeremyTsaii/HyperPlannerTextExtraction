import json
import boto3
from botocore.config import Config

config = Config(s3={"use_accelerate_endpoint": True})
s3 = boto3.client('s3', config=config)

def lambda_handler(event, context):
    BUCKET = 'Bucket Name'
    OBJECT = event['queryStringParameters']['fileName']
    
    getJson = event['queryStringParameters']['getJson']
    
    # Get extracted courses json file
    if getJson == "true":
        try:
            data = s3.get_object(Bucket=BUCKET, Key=OBJECT)
            json_data = json.loads(data['Body'].read().decode('utf-8'))
            status = "found"
        except Exception as e:
            status= "not found"
            print(e)
        
        body = {'status': status, 'data': {}}
        if status == "found":
            body['data'] = json_data
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(body)
        }
    else: # Get presigned url for user to upload
        url = s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': BUCKET, 'Key': OBJECT, 'ContentType': 'application/pdf'},
            ExpiresIn=120)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(url)
        }
