import boto3
from loguru import logger


def metric_streamer():
    logger.info("Lambda execution started.")
    sqs_client = boto3.resource('sqs', region_name='us-east-2')
    asg_client = boto3.client('autoscaling', region_name='us-east-2')
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-2')

    AUTOSCALING_GROUP_NAME = 'saraa-asg'
    QUEUE_NAME = 'saraa-predictionReq-queue'

    queue = sqs_client.get_queue_by_name(QueueName=QUEUE_NAME)
    msgs_in_queue = int(queue.attributes.get('ApproximateNumberOfMessages'))
    asg_groups = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[AUTOSCALING_GROUP_NAME])[
        'AutoScalingGroups']

    if not asg_groups:
        raise RuntimeError('Autoscaling group not found')
    else:
        asg_size = asg_groups[0]['DesiredCapacity']

    backlog_per_instance = msgs_in_queue / asg_size

    # Send metric to CloudWatch
    logger.info(f"Sending metric to CloudWatch. Backlog per instance: {backlog_per_instance}")
    cloudwatch_client.put_metric_data(
        Namespace='saraa-scale-in-out',
        MetricData=[
            {
                'MetricName': 'BacklogPerInstance',
                'Value': backlog_per_instance,
                'Unit': 'Count'
            },
        ]
    )
    logger.info("Lambda execution completed successfully.")
