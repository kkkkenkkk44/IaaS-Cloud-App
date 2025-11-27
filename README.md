# Elastic Face Recognition Service (IaaS)

This project implements an elastic, auto-scaling face recognition service on AWS. It uses a hybrid IaaS/PaaS architecture to handle variable workloads efficiently, scaling from zero to high throughput (1000 req/s).

## Architecture

The system consists of two main tiers:

### 1. Web Tier (`web-tier/`)
- **Server (`server.py`)**: A Flask-based web server that handles incoming HTTP requests.
    - **Streaming Uploads**: Streams large images directly to S3 to minimize memory usage.
    - **Async Processing**: Uses a thread pool (100 workers) to handle high concurrency.
    - **State Management**: Tracks request status using SQS and S3.
- **Controller (`controller.py`)**: An intelligent auto-scaler.
    - **Worker-Driven Scaling**: Only handles **Scale Up** logic.
    - **Scale Down**: Delegated to workers to prevent race conditions (workers stop themselves when idle).

### 2. App Tier (`app-tier/`)
- **Backend (`backend.py`)**: The worker node that performs face recognition.
    - **Batch Processing**: Fetches messages in batches of 10 from SQS.
    - **Multiprocessing**: Utilizes all available CPU cores for parallel processing.
    - **Smart Resizing**: Automatically resizes large images (to 1024x1024) before processing to save CPU/RAM.
    - **Self-Termination**: Automatically shuts down the EC2 instance when the queue is empty to save costs.

## Workflow

1.  **User** uploads an image to the Web Tier.
2.  **Web Server** streams the image to S3 (Input Bucket) and pushes a task to SQS (Request Queue).
3.  **Controller** detects the backlog and launches EC2 instances (App Tier).
4.  **Backend Workers** pull tasks from SQS, download images from S3, and perform face recognition.
5.  **Results** are saved to S3 (Output Bucket) and pushed to SQS (Response Queue).
6.  **Web Server** retrieves the result and returns it to the user.

## Setup & Usage

### Prerequisites
- AWS Account with S3 Buckets and SQS Queues created.
- Python 3.8+
- EC2 Instances with appropriate IAM roles.

### Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/your-username/iaas-cloud-app.git
    cd iaas-cloud-app
    ```

2.  Install dependencies:
    ```bash
    pip install boto3 flask werkzeug pillow
    # Install PyTorch and Facenet (see app-tier/facenet_pytorch/README.md)
    ```

### Running the Web Tier
```bash
cd web-tier
python3 server.py
# In another terminal
python3 controller.py
```

### Running the App Tier
Deploy `backend.py` to your EC2 instances. It is recommended to use a User Data script to start it automatically on boot.
```bash
cd app-tier
python3 backend.py
```

## Optimizations
- **High Throughput**: Capable of handling 1000 req/s with `c5.xlarge` instances and ALB.
- **Cost Efficient**: "Scale to Zero" architecture ensures you don't pay for idle resources.
- **Robustness**: Handles large file uploads and prevents accidental worker termination.
