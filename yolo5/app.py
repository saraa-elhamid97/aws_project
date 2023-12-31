import time
from pathlib import Path
from detect import run
import yaml
from loguru import logger
import os
import boto3
import requests


images_bucket = os.environ['BUCKET_NAME']
queue_name = os.environ['SQS_QUEUE_NAME']
telegram_url = os.environ['TELEGRAM_APP_URL']
sqs_client = boto3.client('sqs', region_name='us-east-2')
dynamodb = boto3.client('dynamodb', region_name='us-east-2')


with open("data/coco128.yaml", "r") as stream:
    names = yaml.safe_load(stream)['names']


def consume():
    while True:
        response = sqs_client.receive_message(QueueUrl=queue_name, MaxNumberOfMessages=1, WaitTimeSeconds=5)

        if 'Messages' in response:
            prediction_req = response['Messages'][0]['Body']
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            # Use the MessageId as a prediction UUID
            prediction_id = response['Messages'][0]['MessageId']

            # prediction_req consists of image name to download from S3 and chat id
            img_name = prediction_req.split(' ')[0]
            chat_id = prediction_req.split(' ')[1]
            original_img_path = img_name

            s3 = boto3.client('s3')
            try:
                logger.info(f'prediction: {prediction_id}. start processing')
                s3.download_file(images_bucket, img_name, original_img_path)
                logger.info(f'prediction: {prediction_id}/{original_img_path}. Download img completed')
            except Exception as e:
                logger.error(f"Failed to download {img_name}. Error: {str(e)}")
                return f'Server Error', 500

            # Predicts the objects in the image
            run(
                weights='yolov5s.pt',
                data='data/coco128.yaml',
                source=original_img_path,
                project='static/data',
                name=prediction_id,
                save_txt=True
            )

            logger.info(f'prediction: {prediction_id}/{original_img_path}. done')

            # This is the path for the predicted image with labels. The predicted image typically includes bounding
            # boxes drawn around the detected objects, along with class labels and possibly confidence scores.
            predicted_img_path = Path(f'static/data/{prediction_id}/{original_img_path}')
            predicted_img = "predicted_" + img_name
            try:
                logger.info(f"Start uploading {predicted_img} to s3")
                s3.upload_file(predicted_img_path, images_bucket, predicted_img)
                logger.info(f"Successfully uploaded {predicted_img} to s3")
            except Exception as e:
                logger.error(f"Failed to upload {predicted_img}. Error: {str(e)}")
                return f'Server Error', 500

            # Parse prediction labels and create a summary
            pred_summary_path = Path(f'static/data/{prediction_id}/labels/{original_img_path.split(".")[0]}.txt')
            if pred_summary_path.exists():
                with open(pred_summary_path) as f:
                    labels = f.read().splitlines()
                    labels = [line.split(' ') for line in labels]
                    labels = [{
                        'class': names[int(l[0])],
                        'cx': float(l[1]),
                        'cy': float(l[2]),
                        'width': float(l[3]),
                        'height': float(l[4]),
                    } for l in labels]

                logger.info(f'prediction: {prediction_id}/{original_img_path}. prediction summary:\n\n{labels}')
                # Convert each dictionary in labels to a DynamoDB map
                labels_list = [
                    {
                        'M': {
                            'class': {'S': label['class']},
                            'cx': {'N': str(label['cx'])},
                            'cy': {'N': str(label['cy'])},
                            'width': {'N': str(label['width'])},
                            'height': {'N': str(label['height'])}
                        }
                    }
                    for label in labels
                ]
                prediction_summary = {
                    'prediction_id': {'S': prediction_id},
                    'original_img_path': {'S': original_img_path},
                    'predicted_img_path': {'S': str(predicted_img_path)},
                    'labels': {'L': labels_list},
                    'time': {'N': str(time.time())}
                }

                # Store the prediction_summary in a DynamoDB table
                logger.info(f'Store prediction summary in DynamoDb table start')
                dynamodb.put_item(
                    TableName='saraa-detected-objects',
                    Item={
                        'prediction_id': {'S': prediction_id},
                        'prediction_summary': {'M': prediction_summary},
                        'chat_id': {'S': chat_id}
                    }
                )
                logger.info(f'prediction summary successfully stored in dynamodb table')
                # Perform a GET request to Polybot to `/results` endpoint
                requests.get(f"https://{telegram_url}/results?predictionId={prediction_id}", verify='self-signed'
                                                                                                    '-certificate.pem')
                # Delete the message from the queue as the job is considered as DONE
                sqs_client.delete_message(QueueUrl=queue_name, ReceiptHandle=receipt_handle)


if __name__ == "__main__":
    consume()
