import pandas as pd
import pika
import time
import json
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str,
                        help='Set the client ID.', default='cus1')
    return parser.parse_args()


args = parse_args()
topics = ["clientData"]
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters(
    'rabbit-server1', 5672, '/', credentials)
pd_data = pd.read_csv("../data/data.csv")
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
topicName = topics[0]

for i in range(10000):
    data_row = pd_data.sample().to_dict(orient='records')[0]
    channel.basic_publish(
        exchange='', routing_key=topicName, body=json.dumps(data_row))


def callback(ch, method, properties, body):
    print("Received"+body.decode(), file=open("../logs/rabbitmq_client.log", "a"))


channel.queue_declare(queue="receivePredictedFareClient1")
channel.basic_consume(queue="receivePredictedFareClient1",
                      on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages on topic',
      "receivePredictedFareClient1", 'To exit press CTRL+C')
print(' [*] Waiting for messages on topic', "receivePredictedFareClient1",
      'To exit press CTRL+C', file=open("../logs/rabbitmq_client.log", "a"))
channel.start_consuming()
