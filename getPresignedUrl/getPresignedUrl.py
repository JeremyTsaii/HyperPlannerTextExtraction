import json
import boto3
from botocore.config import Config

config = Config(s3={"use_accelerate_endpoint": True})
s3_client = boto3.client('s3', config=config)

def lambda_handler(event, context):
    BUCKET = 'Bucket Name'
    OBJECT = event['queryStringParameters']['fileName']
    
    url = s3_client.generate_presigned_url(
        'put_object',
        Params={'Bucket': BUCKET, 'Key': OBJECT},
        ExpiresIn=300)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(url)
    }
