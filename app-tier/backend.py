import boto3
import subprocess
import os
import time

# AWS Configuration
ASU_ID = "1231931599"  # Replace with your ASU ID
S3_INPUT_BUCKET = f"{ASU_ID}-in-bucket"
S3_OUTPUT_BUCKET = f"{ASU_ID}-out-bucket"
SQS_REQUEST_QUEUE = f"{ASU_ID}-req-queue"
SQS_RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"

# Local directory to store downloaded images
LOCAL_IMAGE_DIR = "images/"

# Ensure the directory exists before starting
if not os.path.exists(LOCAL_IMAGE_DIR):
    os.makedirs(LOCAL_IMAGE_DIR, exist_ok=True)
    print(f"Created missing directory: {LOCAL_IMAGE_DIR}")

AWS_REGION = "us-east-1"  # Change to your region

# Initialize AWS Clients with region_name
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Get SQS Queue URLs
req_queue_url = sqs_client.get_queue_url(QueueName=SQS_REQUEST_QUEUE)["QueueUrl"]
resp_queue_url = sqs_client.get_queue_url(QueueName=SQS_RESPONSE_QUEUE)["QueueUrl"]

def get_pending_request_count():
    """Returns the approximate number of pending requests in the SQS request queue."""
    response = sqs_client.get_queue_attributes(
        QueueUrl=req_queue_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(response["Attributes"]["ApproximateNumberOfMessages"])


import urllib.request

def get_instance_id():
    """Retrieves the EC2 instance ID from the metadata service."""
    try:
        # IMDSv2 is preferred, but for simplicity/compatibility we'll try v1 first or just standard retrieval
        # This URL is standard for EC2 instance metadata
        return urllib.request.urlopen("http://169.254.169.254/latest/meta-data/instance-id", timeout=1).read().decode()
    except Exception as e:
        print(f"Could not retrieve instance ID: {e}")
        return None

import multiprocessing
from functools import partial

def process_single_message(message):
    """Helper function to process a single SQS message."""
    try:
        filename = message["Body"]
        receipt_handle = message["ReceiptHandle"]
        
        # Extract image name without extension
        image_name = os.path.splitext(filename)[0]
        
        # Define local file path (ensure unique path for parallel processing if needed, 
        # but here filename is unique so it's fine)
        local_image_path = os.path.join(LOCAL_IMAGE_DIR, filename)
        
        # Download image from S3
        success = download_image_from_s3(filename, local_image_path)
        if not success:
            print(f"Error: Failed to download {filename} from S3. Skipping request.")
            # Delete failed message to avoid retry loop
            sqs_client.delete_message(QueueUrl=req_queue_url, ReceiptHandle=receipt_handle)
            return
            
        print(f"Downloaded image from S3: {local_image_path}")
        
        # Optimize: Resize image if it's too large
        resize_image(local_image_path)
        
        # Perform face recognition
        recognition_result = execute_face_recognition(local_image_path)
        
        # Delete the image after processing
        if os.path.exists(local_image_path):
            os.remove(local_image_path)
            print(f"Deleted local image: {local_image_path}")
            
        # Store result in S3 output bucket
        s3_client.put_object(Bucket=S3_OUTPUT_BUCKET, Key=image_name, Body=recognition_result)
        print(f"Stored result in S3: {image_name} -> {recognition_result}")
        
        # Send result to response queue
        formatted_result = f"{image_name}:{recognition_result}"
        sqs_client.send_message(QueueUrl=resp_queue_url, MessageBody=formatted_result)
        print(f"Sent result to response queue: {formatted_result}")
        
        # Delete message from SQS request queue
        sqs_client.delete_message(QueueUrl=req_queue_url, ReceiptHandle=receipt_handle)
        
    except Exception as e:
        print(f"Error processing message {message.get('Body', 'unknown')}: {e}")

def process_requests():
    # Get current instance ID for self-termination
    my_instance_id = get_instance_id()
    print(f"Worker started on Instance ID: {my_instance_id}")
    
    # Create a pool of workers. 
    # If on a t2.micro (1 vCPU), this won't help much with CPU-bound tasks, 
    # but helps with I/O (downloading/uploading).
    # On larger instances (c5.xlarge), this utilizes all cores.
    pool_size = multiprocessing.cpu_count()
    print(f"Starting multiprocessing pool with {pool_size} workers")
    
    with multiprocessing.Pool(processes=pool_size) as pool:
        while True:
            pending_requests = get_pending_request_count()
            print(f"Current pending requests in SQS: {pending_requests}")
            
            # Batch receive up to 10 messages
            response = sqs_client.receive_message(
                QueueUrl=req_queue_url, 
                MaxNumberOfMessages=10, 
                WaitTimeSeconds=5
            )
            
            if "Messages" in response:
                messages = response["Messages"]
                print(f"Received batch of {len(messages)} messages")
                
                # Process messages in parallel
                pool.map(process_single_message, messages)
                
            else:
                # No messages found.
                print("No new requests in SQS.")
                
                if my_instance_id:
                    print(f"Instance {my_instance_id} is idle. Stopping self...")
                    try:
                        ec2_client.stop_instances(InstanceIds=[my_instance_id])
                        print("Stop command sent. Shutting down...")
                        break 
                    except Exception as e:
                        print(f"Failed to stop instance: {e}")
                        time.sleep(10)
                else:
                    print("Running locally or cannot get instance ID. Sleeping for 10 seconds...")
                    time.sleep(10)

if __name__ == "__main__":
    process_requests()



def download_image_from_s3(filename, local_path):
    """Downloads an image from S3 only if it exists."""
    try:
        # Check if the file exists in S3
        s3_client.head_object(Bucket=S3_INPUT_BUCKET, Key=filename)

        # If the file exists, proceed with the download
        s3_client.download_file(S3_INPUT_BUCKET, filename, local_path)
        return True
    except s3_client.exceptions.ClientError as e:
        # If the error is a 404 (Not Found), log and return False
        if e.response["Error"]["Code"] == "404":
            print(f"Error: {filename} does not exist in S3.")
        else:
            print(f"Error downloading {filename} from S3: {e}")
        return False


def execute_face_recognition(image_path):
    """Executes face_recognition.py by passing the image file path."""
    try:
        command = ["python3", "face_recognition.py", image_path]
        print(f"Executing face recognition command: {' '.join(command)}")

        # Run the face recognition process
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result, error = process.communicate()

        # Print debugging output
        print(f"Face recognition output: {result.decode('utf-8').strip()}")
        print(f"Face recognition error (if any): {error.decode('utf-8').strip()}")

        # Return the output if successful
        if process.returncode == 0:
            return result.decode("utf-8").strip()
        else:
            print(f"Error in face_recognition.py, return code: {process.returncode}")
            return "Error"

    except Exception as e:
        print(f"Error executing face_recognition.py: {e}")
        return "Error"

from PIL import Image

def resize_image(image_path, max_size=(1024, 1024)):
    """Resizes the image to fit within max_size while maintaining aspect ratio."""
    try:
        with Image.open(image_path) as img:
            # Check if resizing is needed
            if img.width > max_size[0] or img.height > max_size[1]:
                print(f"Resizing image {image_path} from {img.size} to fit {max_size}")
                img.thumbnail(max_size)
                img.save(image_path)
                print(f"Image resized to {img.size}")
            else:
                print(f"Image {image_path} is within limits {img.size}, skipping resize.")
    except Exception as e:
        print(f"Warning: Failed to resize image {image_path}: {e}")
        # Continue even if resize fails, as the original image might still work

if __name__ == "__main__":
    process_requests()
