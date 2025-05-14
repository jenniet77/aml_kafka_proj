# This consumer will...
# Consume transactions from Kafka producer
# Log receipt of transactions

from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import time

def process_transaction(tx):
    print(f"Received: {tx['transaction_id']} | Type: {tx['type']} | Amount: {tx['amount']}")

def main():
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'aml-consumer-group',
        'auto.offset.reset': 'earliest', # start from beginning
        'enable.auto.commit': True
    }

    consumer = Consumer(consumer_conf)
    topic = 'transactions'
    consumer.subscribe([topic])

    print(f"Subscribed to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                transaction = json.loads(msg.value().decode('utf-8'))
                process_transaction(transaction)

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()