import json
import numpy as np
import redis.exceptions
import torch
from astropy.io import fits
from redis import Redis
import redis
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import sys
import os
from urllib.parse import urlparse 

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT","localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY","minioadmin")
WORK_QUEUE_NAME = os.environ.get("WORK_QUEUE_NAME","workQueue")
DLQ_NAME =  os.environ.get("DLQ_NAME","dead_letter_queue")
PROCESSED_DATA_BUCKET = os.environ.get("PROCESSED_DATA_BUCKET","user-data")
MAX_RETRIES = 3



# MAIN OBJECTIVES
# 1. create an connect to redis client (+)
# 2. try fetching work from queue if failed again push in queue ()
# 3. fetch fits img from minio bucket (+)
# 4. preprocess the data (+)
# 5. add to the other minio bucket (+)

# ------------------------------HELPER FUNCTIONS--------------------------------

def createS3_client():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url = f'http://{MINIO_ENDPOINT}',
            aws_access_key_id = MINIO_ACCESS_KEY,
            aws_secret_access_key = MINIO_SECRET_KEY,
            config = Config(signature_version = 's3v4')
        )
        return s3_client        
    except ClientError as e:
        print(f"Error creating S3 client: {e}", file=sys.stderr)
        return None


def fetchFile(s3_client, s3_address, local_path):
    try:
        parsed_url = urlparse(s3_address)
        if parsed_url.scheme != 's3':
            raise ValueError("S3 URI must start with 's3://'")

        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')

        if not bucket_name or not object_key:
            raise ValueError("S3 URI is incomplete. It must include a bucket and object key.")
        
        destination_dir = os.path.dirname(local_path)
        if destination_dir:
            os.makedirs(destination_dir, exist_ok=True)
            
        s3_client.download_file(bucket_name, object_key, local_path)
        return True

    except ValueError as e:
        print(f"Invalid Input Error: {e}", file=sys.stderr)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Error: The object '{object_key}' was not found in bucket '{bucket_name}'.", file=sys.stderr)
        else:
            print(f"An S3 client error occurred: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return False
    
# bucket_name -> common bucket hoga, objectLocation ->userId/jobId
def uploadTensor(s3_client, tensorFileLocation, bucket_name, objectLocation):
    s3_client.upload_file(tensorFileLocation,bucket_name,objectLocation)
    

def preprocessData(input_local_path, output_local_path):
    # LOADING THE DATA
    #input_local_path = "frame-g-001000-1-0027.fits"
    hdul = fits.open(input_local_path)

    data = hdul[0].data

    # NORMALIZING DATA
    pmin, pmax = np.percentile(data, (2,98.5))
    data = (data-pmin)/(pmax-pmin)

    # ADD NOISE
    noise_sigma = 0.01
    noise = np.random.normal(0,noise_sigma, data.shape)
    data += noise

    # RANDOM L/R OR U/D
    if np.random.rand() > 0.42:
        data = np.fliplr(data)
    if np.random.rand() > 0.42:
        data = np.flipud(data)
        
    # CONVERTING THE NUMPY NDARRAY TO PYTORCH TENSOR
    data = np.ascontiguousarray(data) #this will prevent from crashing as after random flip it is possible we can have -ve values 
    #isliye we just make another contiguous copy
    DataTensor = torch.from_numpy(data)
    DataTensor = DataTensor.unsqueeze(0)

    torch.save(DataTensor, output_local_path)
    #return DataTensor

# ------------------------------MAIN WORKER CODE--------------------------------
def main():
    # try connnecting to redis 
    print("Connecting to redis...")
    try:
        redis_conn = Redis(decode_responses=True)
        redis_conn.ping()
    except redis.exceptions.ConnectionError as e:
        print("Failed connecting to redis queue")
        return
    
    while True:
        try:
            job_data = redis_conn.brpop(WORK_QUEUE_NAME,0)
            job_info_string = (job_data[1])
            job_info = json.loads(job_info_string)
            userId, jobId, s3Address = job_info['userId'], job_info['jobId'], job_info['s3Address']
            
            s3 = createS3_client()
            
            local_fits_path = f"/tmp/{jobId}.fits" #f'fetched_data/{jobId}.fits'
            output_local_path = f"/tmp/{jobId}.pt" #f'saved_tensor/{jobId}.pt'
            
            if not fetchFile(s3,s3Address,local_fits_path):
                raise RuntimeError(f"Download failed!!")
            
            preprocessData(local_fits_path, output_local_path)
            
            final_path = f'{userId}/{jobId}.pt'
            uploadTensor(s3,output_local_path, PROCESSED_DATA_BUCKET ,final_path)
            
            #clean up
            os.remove(local_fits_path)
            os.remove(output_local_path)
         
        # these error are bad so remove these jobs    
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            if job_info_string:
                redis_conn.lpush(DLQ_NAME,job_info_string)
        
        # these error are worker side errors so push back the job in workQueue
        except Exception as e:
                try:
                    # Try to parse the json to add the counter
                    job_payload = json.loads(job_info_string)
                    retries = job_payload.get('retries', 0)
                    
                    if retries < MAX_RETRIES: # Only retry a maximum of 3 times
                        job_payload['retries'] = retries + 1
                        requeue_string = json.dumps(job_payload)
                        print(f"Re-queueing job (attempt {retries + 1}).")
                        redis_conn.lpush(WORK_QUEUE_NAME, requeue_string)
                    else:
                        print(f"PERMANENT FAILURE: Job failed after 3 retries. Moving to DLQ.")
                        redis_conn.lpush(DLQ_NAME, job_info_string)

                except json.JSONDecodeError:
                    # If it's not even valid JSON, it's a poison pill for sure.
                    print("Could not parse job to add retry count. Moving to DLQ.")
                    redis_conn.lpush(DLQ_NAME, job_info_string)
            

    
if __name__ == "__main__":
    main()