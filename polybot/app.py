import flask
from flask import request
import os
from bot import ObjectDetectionBot
import boto3
import json
app = flask.Flask(__name__)


# Load TELEGRAM_TOKEN value from Secret Manager
session = boto3.session.Session()
secret_manager = session.client('secretsmanager', region_name='us-east-2')
secret_name = "saraa-telegram-token"
TELEGRAM_TOKEN = ''
try:
    secret_response = secret_manager.get_secret_value(SecretId=secret_name)
    secret_string = secret_response['SecretString']
    secret_data = json.loads(secret_string)
    TELEGRAM_TOKEN = secret_data['TELEGRAM_TOKEN']
except Exception as e:
    print(f'Error loading telegram token: {e}')

TELEGRAM_APP_URL = os.environ['TELEGRAM_APP_URL']
dynamodb = boto3.client('dynamodb', region_name='us-east-2')

@app.route('/', methods=['GET'])
def index():
    return 'Ok'


@app.route(f'/{TELEGRAM_TOKEN}/', methods=['POST'])
def webhook():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


@app.route(f'/results/', methods=['GET'])
def results():
    prediction_id = request.args.get('predictionId')
    # Retrieve results from DynamoDB and send to the end-user using the prediction_id
    response = dynamodb.get_item(
        TableName='saraa-detected-objects',
        Key={
          'prediction_id': {'S': prediction_id}
        }
    )
    if 'Item' in response.keys():
        chat_id = response['Item']['chat_id']['S']
        prediction_summary = response['Item']['prediction_summary']
        predictions_list = prediction_summary['M']['labels']['L']
        detected_objects = {}
        for predict_item in predictions_list:
            predict = predict_item['M']
            object_name = predict['class']['S']
            if object_name in detected_objects:
                detected_objects[object_name] += 1
            else:
                detected_objects[object_name] = 1
        text_results = 'Detected objects:\n' + '\n'.join(f'{key}: {value}' for key, value in detected_objects.items())
        bot.send_text(chat_id, text_results)
        return 'Ok'
    else:
        return 'Data not found for the provided predictionId', 404


@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL)

    app.run(host='0.0.0.0', port=8443)
