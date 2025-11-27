import boto3
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS Configuration
ASU_ID = "1231931599"  
SQS_REQUEST_QUEUE = f"{ASU_ID}-req-queue"
SQS_RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"  # Added response queue for monitoring
MAX_INSTANCES = 15
SCALE_UP_THRESHOLD = 1  # Start scaling when there's at least one message waiting
SCALE_DOWN_CHECK_PERIOD = 5  # Seconds to wait before checking for scale down

# EC2 Instance Configuration
APP_INSTANCE_PREFIX = "app-tier-instance"
AMI_ID = "ami-066822c130c69c7cd"  # Replace with your actual AMI ID
INSTANCE_TYPE = "t2.micro"
AWS_REGION = "us-east-1"

# Initialize AWS Clients
ec2_client = boto3.client("ec2", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Get SQS Queue URLs
req_queue_url = sqs_client.get_queue_url(QueueName=SQS_REQUEST_QUEUE)["QueueUrl"]
resp_queue_url = sqs_client.get_queue_url(QueueName=SQS_RESPONSE_QUEUE)["QueueUrl"]

def get_queue_metrics():
    """Returns the number of messages waiting in the SQS request queue and in-flight messages."""
    response = sqs_client.get_queue_attributes(
        QueueUrl=req_queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )
    
    visible_messages = int(response["Attributes"].get("ApproximateNumberOfMessages", 0))
    in_flight_messages = int(response["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0))
    
    return visible_messages, in_flight_messages

def get_instances_by_state(state):
    """Returns a list of instance IDs in a given state."""
    response = ec2_client.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [f"{APP_INSTANCE_PREFIX}-*"]},
            {"Name": "instance-state-name", "Values": [state]}
        ]
    )
    return [i["InstanceId"] for r in response["Reservations"] for i in r["Instances"]]

def create_new_instances(count):
    """Creates new instances up to the MAX_INSTANCES limit."""
    if count <= 0:
        return []
    
    # Get all existing instances
    response = ec2_client.describe_instances(
        Filters=[{"Name": "tag:Name", "Values": [f"{APP_INSTANCE_PREFIX}-*"]}]
    )
    
    # Map instance names to IDs
    existing_instances = {}
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            for tag in instance.get("Tags", []):
                if tag["Key"] == "Name" and tag["Value"].startswith(APP_INSTANCE_PREFIX):
                    existing_instances[tag["Value"]] = instance["InstanceId"]
    
    # Find available instance numbers
    created_instance_ids = []
    for i in range(1, MAX_INSTANCES + 1):
        instance_name = f"{APP_INSTANCE_PREFIX}-{i}"
        if instance_name not in existing_instances and len(created_instance_ids) < count:
            logger.info(f"Creating new instance: {instance_name}")
            
            response = ec2_client.run_instances(
                ImageId=AMI_ID,
                MinCount=1,
                MaxCount=1,
                InstanceType=INSTANCE_TYPE,
                KeyName="app-tier-instance",
                SecurityGroups=["launch-wizard-13"],
                IamInstanceProfile={"Name": "EC2WebTierRole"},
                TagSpecifications=[{
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": instance_name}]
                }],
            )
            
            instance_id = response["Instances"][0]["InstanceId"]
            created_instance_ids.append(instance_id)
            
            # Wait for instance to be running
            logger.info(f"Waiting for instance {instance_id} to reach 'running' state...")
            waiter = ec2_client.get_waiter("instance_running")
            waiter.wait(InstanceIds=[instance_id])
            
            # Stop instance immediately (pre-initialize)
            logger.info(f"Stopping instance {instance_id} for pre-initialization...")
            ec2_client.stop_instances(InstanceIds=[instance_id])
            
            # Wait for stopped state
            stop_waiter = ec2_client.get_waiter("instance_stopped")
            stop_waiter.wait(InstanceIds=[instance_id])
            logger.info(f"Instance {instance_id} is now stopped and ready for use.")
    
    return created_instance_ids

def initialize_instances():
    """Ensures instances exist in stopped state up to MAX_INSTANCES."""
    all_instances = get_instances_by_state("running") + get_instances_by_state("stopped") + get_instances_by_state("pending") + get_instances_by_state("stopping")
    
    instances_needed = MAX_INSTANCES - len(all_instances)
    if instances_needed > 0:
        logger.info(f"Initializing {instances_needed} new instances...")
        create_new_instances(instances_needed)

def scale_instances():
    """
    Scales instances based on queue load, ensuring they stop immediately when idle.
    Implements optimal scaling strategy with precise workload detection.
    """
    # Ensure we have instances ready in stopped state
    initialize_instances()
    
    logger.info("Auto-scaler started. Monitoring queue for workload...")
    
    while True:
        # Get queue metrics
        visible_messages, in_flight_messages = get_queue_metrics()
        total_workload = visible_messages + in_flight_messages
        
        running_instances = get_instances_by_state("running")
        pending_instances = get_instances_by_state("pending")  # Track instances that are starting
        stopping_instances = get_instances_by_state("stopping")  # Track instances that are stopping
        stopped_instances = get_instances_by_state("stopped")
        
        active_instances = len(running_instances) + len(pending_instances)
        
        logger.info(f"Queue: {visible_messages} visible + {in_flight_messages} in-flight = {total_workload} total | " 
                   f"Instances: {len(running_instances)} running, {len(pending_instances)} starting, "
                   f"{len(stopping_instances)} stopping, {len(stopped_instances)} stopped")
        
        # SCALE UP: Start instances when there are more messages than running instances
        if total_workload > active_instances and active_instances < MAX_INSTANCES:
            # Calculate how many new instances we need
            instances_needed = min(total_workload - active_instances, MAX_INSTANCES - active_instances)
            
            if instances_needed > 0 and stopped_instances:
                # Start only the instances we need
                instances_to_start = stopped_instances[:instances_needed]
                logger.info(f"SCALING UP: Starting {len(instances_to_start)} instances to handle {total_workload} messages")
                ec2_client.start_instances(InstanceIds=instances_to_start)
        
        # SCALE DOWN & SCALE TO ZERO logic has been removed.
        # Instances now self-terminate when they are idle.
        # This prevents the controller from accidentally stopping an instance that is processing a request.
        
        # Dynamic sleep time - slower polling when idle, faster when active
        sleep_time = 1 if total_workload > 0 else 5
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        scale_instances()
    except KeyboardInterrupt:
        logger.info("Auto-scaler terminated by user.")
    except Exception as e:
        logger.error(f"Error in auto-scaler: {str(e)}", exc_info=True)