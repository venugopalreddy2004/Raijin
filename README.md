# Project Raijin: A Distributed Data Processing Pipeline


**Tech Stack:** `Python`, `TypeScript`, `Node.js`, `Express.js`, `Docker`, `Docker Compose`, `Redis`, `S3 (MinIO)`, `PyTorch`, `Astropy`

---

## 1. Problem Statement

In many scientific and large-scale Machine Learning workflows, the GPU—the most expensive computational resource—is often starved for data. The process of loading, preprocessing, and augmenting specialized data files (like astronomical FITS images) is I/O-bound and slow. This creates a bottleneck where the GPU sits idle, waiting for data, which wastes significant time and computational resources.

This project solves this problem by engineering a distributed, asynchronous backend system that decouples data preprocessing from model training. The platform processes data in the background and in parallel, creating a high-throughput pipeline that ensures a model's training loop is always fed, maximizing GPU utilization.

## 2. Architecture

Raijin is a **polyglot microservices application** built on the asynchronous **Producer/Consumer pattern**. The entire backend infrastructure is containerized and defined in a single Docker Compose file for easy, reproducible deployment.


### Core Components:

*   **API Gateway (TypeScript/Node.js):** A lightweight, asynchronous primary backend built with Express.js. It serves as the secure front door for the system, handling API key authentication, request validation, and pushing jobs into the task queue.
*   **Task Queue (Redis):** A Redis list serves as the central message broker, decoupling the API Gateway from the processing workers. It includes a **Dead-Letter Queue (DLQ)** pattern to isolate and save malformed or repeatedly failing jobs for analysis.
*   **Python Workers:** A scalable pool of stateless, containerized Python services. Each worker pulls a job from Redis, downloads the source FITS file from object storage, executes a multi-step scientific preprocessing pipeline (using Astropy, NumPy, and PyTorch), and uploads the resulting tensor to a private, user-specific location.
*   **Python SDK (`AstroDataLoader`):** An elegant, easy-to-use client library that completely abstracts away the backend complexity. It features a background thread that manages job submission and result collection, providing a simple, iterable, pre-fetching data loader with a `tqdm` progress bar for any standard PyTorch training loop.
*   **Object Storage (MinIO):** An S3-compatible server for storing all data assets.

## 3. Key Features

*   **Asynchronous & Non-Blocking:** The SDK's pre-fetching mechanism ensures that data is processed in the background, so the main training loop never waits for I/O.
*   **Scalable by Design:** The stateless nature of the workers allows the system's throughput to be scaled horizontally by adding more worker replicas with a single `docker-compose` command.
*   **Polyglot Architecture:** Intelligently uses the best tool for the job: **TypeScript/Node.js** for high-performance network I/O at the API Gateway, and **Python** for its unrivaled scientific computing ecosystem in the workers.
*   **Resilient:** The workers implement a retry mechanism for transient failures and move "poison pill" jobs to a Dead-Letter Queue, ensuring the system doesn't halt on bad data.
*   **Fully Containerized:** The entire multi-service application is orchestrated with Docker Compose, enabling a complete, one-command setup for the entire development environment.

## 4. How to Run

This project is fully containerized. The only prerequisites are **Docker** and **Docker Compose**.

### Step 1: Data Preparation

A script is provided to automatically download a ~6 GB benchmark dataset from the Sloan Digital Sky Survey.

```bash
# (Optional) Install Python dependencies for the script if not already present
# pip install requests beautifulsoup4 pandas tqdm

# Run the data preparation script from the project root
python ./data_pull/prepare_dataset.py 
```
This will create a `sdss_benchmark_dataset` folder containing 500 FITS files and the required `manifest.csv`.

### Step 2: Configure the Environment

Create an environment file for Docker Compose from the provided template. The default values are pre-configured for the local development setup and do not need to be changed.

```bash
cp .env.template .env
```

### Step 3: Launch the Backend

Launch the entire backend stack (API, Workers, Redis, MinIO) with a single command. The `initialize-minio` service will automatically create the required buckets and upload the seed data from the `sdss_benchmark_dataset` folder.

```bash
# Build all images and run the services in the background.
# The '--scale' flag can be used to set the number of workers.
docker-compose up --build --force-recreate -d --scale unit-worker=4
```
You can view the real-time logs of all services by running: `docker-compose logs -f`

### Step 4: Test the System

The system is now running. You can interact with it using the Python SDK. Create a test file (e.g., `test_sdk.py`) with the following content:

```python
# file: test_sdk.py
from sdk.data_pipeline import AstroDataLoader
import time

print("Initializing the AstroDataLoader...")

# The loader will use the configuration from your local setup
loader = AstroDataLoader(
    manifest_s3_path="s3://dataset/manifest.csv",
    api_endpoint="http://localhost:8081",
    api_key="super-secret-key-42",
    batch_size=8
)

print("Starting the processing loop...")
# The loader will automatically display a tqdm progress bar
for i, batch in enumerate(loader):
    # print(f"Main script received BATCH #{i+1} with shape: {batch.shape}")
    time.sleep(0.1) # Simulate training work

print("Success! The SDK has processed the entire dataset.")
```
Now, run the test script from your project's root directory:
```bash
python test_sdk.py
```
You will see the loader initialize, followed by a real-time progress bar tracking the delivery of all 500 images.

## 5. Project Status & Next Steps

The core system, including the multi-service backend and the Python SDK, is **feature-complete and fully functional.**

The next and final phase of the project is to conduct a formal performance validation.

*   **[TODO - Future Work] Migrate to Kubernetes:** As a follow-on, the Docker Compose configuration can be translated into Kubernetes manifests to enable production-grade features like self-healing and automatic scaling with KEDA.