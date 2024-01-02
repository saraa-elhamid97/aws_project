import boto3


def lambda_handler(event, context):
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

    backlog_per_instance = 7  if msgs_in_queue > 0 and asg_size == 0 else msgs_in_queue / asg_size

    # Send metric to CloudWatch
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
