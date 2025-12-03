import os
import boto3
import time
import threading
import uuid
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Configuration
ASU_ID = "1231931599"  # Replace with your ASU ID
S3_BUCKET_NAME = f"{ASU_ID}-in-bucket"
SQS_REQUEST_QUEUE = f"{ASU_ID}-req-queue"
SQS_RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"
AWS_REGION = "us-east-1"

# Initialize AWS Clients with resource pooling
session = boto3.Session(region_name=AWS_REGION)
s3_client = session.client('s3')
sqs_client = session.client('sqs')

# Flask Web Server
app = Flask(__name__)

import redis

# ... (imports remain the same)

# Redis Configuration
# In production, use environment variables for host/port
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# ... (AWS config remains the same)

# Flask Web Server
app = Flask(__name__)

# Thread pool for handling concurrent requests
# Increased to 100 to handle higher throughput (100-1000 req/s)
executor = ThreadPoolExecutor(max_workers=100)

# Get SQS Queue URLs
req_queue_url = sqs_client.get_queue_url(QueueName=SQS_REQUEST_QUEUE)["QueueUrl"]
resp_queue_url = sqs_client.get_queue_url(QueueName=SQS_RESPONSE_QUEUE)["QueueUrl"]

# ... (SQS size check remains the same)

# Start background thread for response polling
def response_poller():
    """Background thread that continuously polls for responses and updates Redis"""
    logger.info("Response poller started")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=resp_queue_url,
                MaxNumberOfMessages=10,  # Batch receive messages
                WaitTimeSeconds=5,
                AttributeNames=['MessageGroupId', 'MessageDeduplicationId']
            )
            
            if "Messages" in response:
                for message in response["Messages"]:
                    body = message["Body"]
                    receipt_handle = message["ReceiptHandle"]
                    
                    # Parse the response (assumes format: "filename:result")
                    parts = body.split(':', 1)
                    if len(parts) == 2:
                        filename, result = parts
                        
                        # Update Redis with the result
                        # Set TTL to 300 seconds (5 minutes) to auto-expire old results
                        redis_client.setex(filename, 300, result)
                        logger.info(f"Updated Redis for: {filename}")
                    
                    # Delete processed message
                    sqs_client.delete_message(
                        QueueUrl=resp_queue_url,
                        ReceiptHandle=receipt_handle
                    )
        
        except Exception as e:
            logger.error(f"Error in response poller: {e}")
            time.sleep(1)  # Prevent tight loop in case of repeated errors

# Process a single upload
def process_upload(file):
    try:
        # Generate unique filename to prevent collisions
        original_filename = secure_filename(file.filename)
        filename_parts = os.path.splitext(original_filename)
        unique_filename = f"{filename_parts[0]}_{uuid.uuid4().hex[:8]}{filename_parts[1]}"
        
        # Set initial status in Redis (PENDING)
        # This acts as the "ticket"
        redis_client.setex(unique_filename, 300, "PENDING")
        
        # Upload the file to S3
        # Note: upload_fileobj automatically streams the file data, avoiding memory issues with large files.
        logger.info(f"Starting S3 upload for: {unique_filename}")
        upload_start_time = time.time()
        s3_client.upload_fileobj(file, S3_BUCKET_NAME, unique_filename)
        upload_duration = time.time() - upload_start_time
        logger.info(f"Uploaded image to S3: {unique_filename} in {upload_duration:.2f} seconds")
        
        # Send only the filename to SQS request queue
        response = sqs_client.send_message(
            QueueUrl=req_queue_url,
            MessageBody=unique_filename
        )
        logger.info(f"Sent request to SQS: {unique_filename}, Message ID: {response['MessageId']}")
        
        # Wait for response with timeout
        start_time = time.time()
        timeout = 60  # 60 second timeout
        
        while time.time() - start_time < timeout:
            # Poll Redis for result
            result = redis_client.get(unique_filename)
            
            if result and result != "PENDING":
                # Result found!
                # Optional: Delete key immediately or let it expire. 
                # Letting it expire allows for "at least once" delivery if client retries.
                redis_client.delete(unique_filename)
                return {"success": True, "result": result, "filename": original_filename}
            
            time.sleep(0.1)  # Small sleep to prevent tight loop
        
        # Handle timeout
        return {"success": False, "error": "Request timed out", "filename": original_filename}
    
    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}")
        return {"success": False, "error": str(e), "filename": file.filename}

@app.route("/", methods=["POST"])
def handle_request():
    """Handles HTTP POST requests asynchronously"""
    if "inputFile" not in request.files:
        return jsonify({"error": "Missing 'inputFile' in request"}), 400
    
    file = request.files["inputFile"]
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400
    
    # Submit the task to the thread pool and return the result
    future = executor.submit(process_upload, file)
    result = future.result()
    
    if result["success"]:
        return f"{result['result']}", 200
    else:
        return f"Error: {result['error']}", 500

@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    try:
        redis_client.ping()
        return "OK", 200
    except Exception as e:
        return f"Redis Error: {e}", 500

if __name__ == "__main__":
    # Start background threads
    threading.Thread(target=response_poller, daemon=True).start()
    # No need for cleanup thread anymore, Redis TTL handles it!
    
    # Start Flask server with optimized settings
    logger.info("Starting web tier server on port 8000")
    app.run(
        host="0.0.0.0", 
        port=8000, 
        threaded=True,
        debug=False  # Disable debug mode in production
    )