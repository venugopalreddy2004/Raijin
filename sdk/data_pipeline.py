import sys
import os
import queue
import time
import pandas as pd
import requests 
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import threading
import torch
from tqdm import tqdm 



MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

class Astro_loader:
    def __init__(self, api_endpoint=None, api_key=None, manifest_s3_address=None, batch_size=32):
        self.batch_size = batch_size
        self.api_endpoint = api_endpoint
        self.api_key = api_key
        self.manifest_address = manifest_s3_address
        self.progress_bar = None 
        self._load()
        
        
        self.worker_thread = threading.Thread(target=self._background_worker_loop, daemon=True)
        self.worker_thread.start()
        
        print("Loader was successfully initialized")
        
    def _fetchData(self, s3_client):
        try:
            parsed_data = urlparse(self.manifest_address);
            if parsed_data.scheme!='s3':
                raise ValueError("S3 address is wrong")
            
            bucket_name = parsed_data.netloc
            object_key = parsed_data.path.lstrip('/')
            
            if not bucket_name or not object_key:
                raise ValueError("Manifest address in incomplete")
            
            local_path = os.path.join("tmp","manifest.csv")
            if local_path:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
           
            s3_client.download_file(bucket_name,object_key,local_path)
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
    
    def _createS3_client(self):
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url = f'http://{MINIO_ENDPOINT}',
                aws_access_key_id = MINIO_ACCESS_KEY,
                aws_secret_access_key = MINIO_SECRET_KEY,
                config = Config(signature_version = 's3v4')
            )
            self.client = s3_client        
        except ClientError as e:
            print(f"Error creating S3 client: {e}", file=sys.stderr)
            return None
        
    def _load(self):
        self._createS3_client()
        if not self._fetchData(self.client):
            raise RuntimeError("can't download manifest file")
        df = pd.read_csv("tmp/manifest.csv", header=None)
        tasks_list = []
        m,_ = df.shape
        for i in range(m):
            tasks_list.append(df.iloc[i,0])
        self.tasks_list = tasks_list
        self.total_task_count = len(tasks_list)
        self.pending_jobs = {}
        self.ready_tensors = queue.Queue(maxsize=100)
        print("lessgo bhaiii")
        
    def _background_worker_loop(self):
        while self.tasks_list or self.pending_jobs:
            BUFFER_SIZE = 3*self.batch_size
            running_jobs = len(self.pending_jobs) + self.ready_tensors.qsize()
            
            if self.tasks_list and (running_jobs < BUFFER_SIZE):
            
                task_path = (self.tasks_list).pop(0)
                
                
                
                
                payload = {
                    "s3Address" : task_path
                }
                
                headers = {
                    "Authorization" : self.api_key
                }
                
                try:
                    
                    response = requests.post(f"{self.api_endpoint}/submit",json=payload, headers=headers)
                    response.raise_for_status()
                    
                    responseData = response.json()
                    jobId = responseData['taskId']
                    
                    
                    
                    self.pending_jobs[jobId] = responseData['path']
                except Exception as e:
                    print(f"Request failed {e}", file=sys.stderr)
                    
            for jobId in list(self.pending_jobs.keys()):
                objectId = self.pending_jobs[jobId]
                try:
                    self.client.head_object(Bucket="user-data",Key=objectId)
                    local_temp_path = f"/tmp/{jobId}.pt"
                    self.client.download_file(
                        Bucket="user-data",
                        Key=objectId,
                        Filename=local_temp_path
                    )

                    tensor = torch.load(local_temp_path)
                    self.ready_tensors.put(tensor)
                    del self.pending_jobs[jobId]
                    os.remove(local_temp_path)
                
                except ClientError as e:
                    
                    if e.response['Error']['Code'] == '404': 
                        pass
                except Exception as e:
                    print(f"Error handling job {jobId}: {e}", file=sys.stderr)
                    del self.pending_jobs[jobId]
                    self.ready_tensors.put(None)
                    continue

                        
            time.sleep(1)
    
    def __iter__(self):
        self.tasks_served=0
        
        
        self.progress_bar = tqdm(total=self.total_task_count, desc="Processing Images", unit="img")
        return self
    
    def __next__(self):
        
        if self.tasks_served >= self.total_task_count:
            self.progress_bar.close() 
            raise StopIteration

        batch = []
        for _ in range(self.batch_size):
            if self.tasks_served >= self.total_task_count:
                break
            
            try:
                tensor = self.ready_tensors.get(timeout=60)
            except queue.Empty:
                self.progress_bar.close() 
                raise RuntimeError("Timeout: Waited 60s for the next tensor, but the queue is empty.")

            self.tasks_served += 1
            self.ready_tensors.task_done()

            self.progress_bar.update(1)

            if tensor is not None: 
                batch.append(tensor)
        
        if not batch:

            self.progress_bar.close() 
            raise StopIteration

        return torch.stack(batch)
        
    
    

loader = Astro_loader("http://localhost:8081","super-secret-key-42","s3://dataset/sdss_benchmark_dataset/manifest.csv")

for batch_tensor in loader:
    
    
    
    time.sleep(0.1)

print("Processing complete.")