import json
import boto3
import urllib.parse
import kafka
from kafka import KafkaClient
from kafka import KafkaProducer

client = boto3.client('kafka')
s3 = boto3.resource('s3')

"""
This function is triggered on any .json upload to s3 bucket dasnes-mpcs53014
"""

def lambda_handler(event, context):
    producer = KafkaProducer(bootstrap_servers=["b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092","b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092"])
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    task_op = {
        "key": key,
    }
    
    # print(json.dumps(task_op))
    producer.send("topic_dasnes_json_reached_s3",json.dumps(task_op).encode('utf-8'))
    return ("Messages Sent to Kafka Topic")

