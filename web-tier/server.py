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

# Thread pool for handling concurrent requests
# Increased to 100 to handle higher throughput (100-1000 req/s)
executor = ThreadPoolExecutor(max_workers=100)

# Response tracking (maps request ids to response objects)
response_map = {}
response_lock = threading.Lock()

# Get SQS Queue URLs
req_queue_url = sqs_client.get_queue_url(QueueName=SQS_REQUEST_QUEUE)["QueueUrl"]
resp_queue_url = sqs_client.get_queue_url(QueueName=SQS_RESPONSE_QUEUE)["QueueUrl"]

# Verify SQS message size limit
try:
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=req_queue_url, AttributeNames=["MaximumMessageSize"]
    )
    max_message_size = int(queue_attrs["Attributes"]["MaximumMessageSize"])
    if max_message_size > 1024:
        logger.warning(f"Warning: MaximumMessageSize for {SQS_REQUEST_QUEUE} is {max_message_size}. It should be 1024 (1KB).")
    else:
        logger.info(f"SQS queue {SQS_REQUEST_QUEUE} is correctly set to 1KB max message size.")
except Exception as e:
    logger.error(f"Error checking queue attributes: {e}")

# Start background thread for response polling
def response_poller():
    """Background thread that continuously polls for responses and maps them to requests"""
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
                        
                        # Store the result in the response map
                        with response_lock:
                            if filename in response_map:
                                response_map[filename]["result"] = result
                                response_map[filename]["available"] = True
                    
                    # Delete processed message
                    sqs_client.delete_message(
                        QueueUrl=resp_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info(f"Processed response for: {body}")
        
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
        
        # Upload the file to S3
        # Note: upload_fileobj automatically streams the file data, avoiding memory issues with large files.
        logger.info(f"Starting S3 upload for: {unique_filename}")
        upload_start_time = time.time()
        s3_client.upload_fileobj(file, S3_BUCKET_NAME, unique_filename)
        upload_duration = time.time() - upload_start_time
        logger.info(f"Uploaded image to S3: {unique_filename} in {upload_duration:.2f} seconds")
        
        # Initialize response tracking
        with response_lock:
            response_map[unique_filename] = {
                "available": False,
                "result": None,
                "timestamp": time.time()
            }
        
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
            with response_lock:
                if unique_filename in response_map and response_map[unique_filename]["available"]:
                    result = response_map[unique_filename]["result"]
                    # Clean up the response map
                    del response_map[unique_filename]
                    return {"success": True, "result": result, "filename": original_filename}
            
            time.sleep(0.1)  # Small sleep to prevent tight loop
        
        # Handle timeout
        with response_lock:
            if unique_filename in response_map:
                del response_map[unique_filename]
        
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
    return "OK", 200

# Cleanup thread to remove stale entries from response_map
def cleanup_response_map():
    """Background thread that cleans up stale entries in the response map"""
    while True:
        try:
            current_time = time.time()
            with response_lock:
                stale_keys = [
                    k for k, v in response_map.items() 
                    if current_time - v["timestamp"] > 120  # 2 minute timeout
                ]
                for k in stale_keys:
                    del response_map[k]
                    logger.warning(f"Cleaned up stale response for {k}")
        except Exception as e:
            logger.error(f"Error in cleanup thread: {e}")
        
        time.sleep(30)  # Run cleanup every 30 seconds

if __name__ == "__main__":
    # Start background threads
    threading.Thread(target=response_poller, daemon=True).start()
    threading.Thread(target=cleanup_response_map, daemon=True).start()
    
    # Start Flask server with optimized settings
    logger.info("Starting web tier server on port 8000")
    app.run(
        host="0.0.0.0", 
        port=8000, 
        threaded=True,
        debug=False  # Disable debug mode in production
    )