import os
import sys
import subprocess
import logging
import json
import time
import socket
import string
import paho.mqtt.client as paho

qos=2

logging.basicConfig(filename='logfile',level=logging.DEBUG,format='%(asctime)-15s %(message)s')
logging.info('Starting')
logging.debug('DEBUG MODE')

topiclist=['dht', 'hall', 'motion', 'mat', 'ene']
mqtt_username='username'
mqtt_password='password'
mqtt_host='localhost'
mqtt_port=1883
mqtt_client_id='launcher1'

def runprog(topic,mess,param=None):
    publish='%s/report' % topic
    if param is not None and all(c in string.printable for c in param)==False:
        logging.debug('Param for topic %s is not printable: skipping' % topic)
        return
    if not topic in topiclist:
        logging.info('Topic %s isn\'t configured' % topic)
    try:
        mess_payload=json.loads(mess)
    except Exception, e:
        print(str(e))
    else:
        client_id=str(mess_payload['id'])
        pub_mess=json.dumps(mess_payload['msg']).replace('"','\\"')
        tstmp=str(mess_payload['time'])
        print(tstmp)
        cmd='aws dynamodb put-item --table-name DynamoDB_LOG --item \'{"timestamp":{"S":"%s"},"client_id":{"S":"%s"},"message":{"S":"%s"},"topic":{"S":"%s"}}\' --return-consumed-capacity TOTAL' % (tstmp,client_id,pub_mess,topic)
        logging.debug('Running t=%s: %s' % (topic,cmd))
        try:
            res=subprocess.check_output(cmd,stdin=None,stderr=subprocess.STDOUT,shell=True,universal_newlines=True)
        except Exception, e:
            res='*****>%s' % str(e)
        payload=res.rstrip('\ ')
        (res,mid)=mqttc.publish(publish,payload,qos=qos,retain=False)

def on_message(mosq,userdata,msg):
    logging.debug(msg.topic+' '+str(msg.qos)+' '+str(msg.payload))
    runprog(msg.topic,str(msg.payload))

def on_connect(mosq,userdata,flags,result_code):
    logging.debug('Connected to MQTT broker, subscribing to topics...')
    for topic in topiclist:
        mqttc.subscribe(topic,qos)

def on_disconnect(mosq,userdata,rc):
    logging.debug('Disconnected')
    time.sleep(10)

if __name__ == '__main__':
    mqttc=paho.Client(mqtt_client_id,clean_session=False)
    mqttc.on_message=on_message
    mqttc.on_connect=on_connect
    mqttc.on_disconnect=on_disconnect
    mqttc.will_set('launcher',payload='Yo!',qos=0,retain=False)
    #mqttc.reconnect_delay_set(delay=3,delay_max=30,exponential_backoff=True)
    mqttc.username_pw_set(mqtt_username,mqtt_password)
    mqttc.connect(mqtt_host,mqtt_port,60)
    while True:
        try:
            mqttc.loop_forever()
        except socket.error:
            time.sleep(5)
        except KeyboardInterrupt:
            sys.exit(0)
