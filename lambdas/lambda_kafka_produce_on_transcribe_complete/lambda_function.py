import json
import boto3
import kafka
from kafka import KafkaClient
from kafka import KafkaProducer

client = boto3.client('kafka')

def lambda_handler(event, context):
    print("in the handler function")
    producer = KafkaProducer(bootstrap_servers="b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092")
    print(producer)
    task_op = {
        "'message": "Hai, Calling from AWS Lambda"
    }
    print(json.dumps(task_op))
    #producer.send_messages("topic_atx_ticket_update",json.dumps(task_op).encode('utf-8'))
    #print(producer.send_messages)
    return ("Messages Sent to Kafka Topic")
