import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

print('Loading function')

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
mqttDhtTable = dynamodb.Table('MQTT_DHT')
mqttHallTable = dynamodb.Table('MQTT_Hall')
mqttMotionTable = dynamodb.Table('MQTT_Motion')
mqttMatTable = dynamodb.Table('MQTT_Mat')
mqttENETable = dynamodb.Table('MQTT_ENE')

def lambda_handler(event, context):
    if event['Records'][0]['eventName'] == 'INSERT':
        data = event['Records'][0]['dynamodb']['NewImage']
        payload = json.loads(data['message']['S'])
        if data['topic']['S'] == 'dht':
            try:
                dhtresponse = mqttDhtTable.put_item(
                    Item = {
                        'timestamp': str(data['timestamp']['S']),
                        'client_id': str(data['client_id']['S']),
                        'temperature': str(payload['t']),
                        'humidity': str(payload['h'])
                    })
                print(dhtresponse)
            except ClientError as e:
                print(e)
                raise e
            else:
                return 'Successfully processed {} records.'.format(len(event['Records']))
        if data['topic']['S'] == 'hall':
            try:
                hallresponse = mqttHallTable.put_item(
                    Item = {
                        'timestamp': str(data['timestamp']['S']),
                        'client_id': str(data['client_id']['S']),
                        'hall': str(payload['t'])
                    })
                print(hallresponse)
            except ClientError as e:
                print(e)
                raise e
            else:
                return 'Successfully processed {} records.'.format(len(event['Records']))
        if data['topic']['S'] == 'smarthome/motion':
            try:
                motionresponse = mqttMotionTable.put_item(
                    Item = {
                        'timestamp': str(data['timestamp']['S']),
                        'client_id': str(data['client_id']['S']),
                        'motion': str(payload['t'])
                    })
                print(motionresponse)
            except ClientError as e:
                print(e)
                raise e
            else:
                return 'Successfully processed {} records.'.format(len(event['Records']))
        if data['topic']['S'] == 'mat':
            try:
                matresponse = mqttMatTable.put_item(
                    Item = {
                        'timestamp': str(data['timestamp']['S']),
                        'client_id': str(data['client_id']['S']),
                        'presence': str(payload['t'])
                    })
                print(matresponse)
            except ClientError as e:
                print(e)
                raise e
            else:
                return 'Successfully processed {} records.'.format(len(event['Records']))
        if data['topic']['S'] == 'ene':
            try:
                eneresponse = mqttENETable.put_item(
                    Item = {
                        'timestamp': str(data['timestamp']['S']),
                        'client_id': str(data['client_id']['S']),
                        'peak_voltage': str(payload['a']),
                        'peak_current': str(payload['b']),
                        'rms_current': str(payload['c']),
                        'wire_current': str(payload['d'])
                    })
                print(eneresponse)
            except ClientError as e:
                print(e)
                raise e
            else:
                return 'Successfully processed {} records.'.format(len(event['Records']))
        return 'No Records Processed'
    print('none')
    return 'No Records Found'
