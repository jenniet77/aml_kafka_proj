# This producer will...
# Connect to Kafka (localhose:9092)
# Generate transaction data
# Send messages to Kafka topic (transactions)
# Log delivery confirmation or errors

import json
import socket
import time
from confluent_kafka import Producer
from transaction_generator import TransactionGenerator

def delivery_report(err, msg):
    """Reports success or failure of a message delivery."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

class TransactionProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='transactions'):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'linger.ms': 10,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 100
            })

    def send_transaction(self, transaction_dict):
        self.producer.produce(
            topic=self.topic,
            key=transaction_dict.get("transaction_id", str(time.time())),
            value=json.dumps(transaction_dict),
            callback=delivery_report
        )
        self.producer.poll(0) # trigger delivery callbacks

    def flush(self):
        self.producer.flush()

if __name__ == "__main__":
    producer = TransactionProducer()
    generator = TransactionGenerator()

    # ---> Activate this block for one-time testing
    # for _ in range(5):
    #     tx = generator.generate_transaction()
    #     print(f"Sending: {tx['transaction_id']}")
    #     producer.send_transaction(tx)
    #     time.sleep(0.5)

    # ---> Activate this block for real-time demo
    try:
        while True:
            tx = generator.generate_transaction()
            print(f"Sending: {tx['transaction_id']}")
            producer.send_transaction(tx)
            time.sleep(5) # send every 5 seconds
    except KeyboardInterrupt:
        print("Producer stopped by user.")
        producer.flush()
