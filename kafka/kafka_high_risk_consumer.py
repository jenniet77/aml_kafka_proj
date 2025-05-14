# This consumer listens to the 'high-risk-transactions' topic and logs flagged frauds

import os
import sys

if '__file__' in globals():
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
else:
    PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), '..'))

sys.path.insert(0, PROJECT_ROOT)

from confluent_kafka import Consumer, KafkaException
import json

class HighRiskConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='high-risk-transactions', group_id='high-risk-monitor'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe([topic])

        print(f"✅ Subscribed to topic '{topic}' for high-risk monitoring")

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                tx = json.loads(msg.value().decode('utf-8'))

                log = f"⚠️ ALERT: {tx['nameOrig']} → {tx['nameDest']} | {tx['type']} | ${tx['amount']:.2f} | " + \
                      f"{tx['fraud_probability']*100:.1f}% probability | Prediction: {tx['fraud_prediction']}"

                print(log)

        except KeyboardInterrupt:
            print("Interrupted. Shutting down high-risk consumer...")
        finally:
            self.consumer.close()
            print("Consumer closed.")

if __name__ == "__main__":
    consumer = HighRiskConsumer()
    consumer.run()