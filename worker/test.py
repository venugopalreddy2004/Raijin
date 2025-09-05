import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

def get_minio_client():
    s3_client = boto3.client(
        's3',
        endpoint_url = f'http://{MINIO_ENDPOINT}',
        aws_access_key_id = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config = Config(signature_version='s3v4')
    )
    #print("Buckets: ", s3_client.list_buckets())
    return s3_client
    
# fetching the client for communication    
s3 = get_minio_client()

#adding to minio s3
#s3.upload_file("requirements.txt","dataset","testTrial/sample.txt")

#fetching from the bucket
s3.download_file("dataset","testTrial/sample.txt","fetched_data/req.txt")