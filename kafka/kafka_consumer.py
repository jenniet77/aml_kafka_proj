# This consumer will...
# Consume transactions from Kafka producer
# Log receipt of transactions
# Perform fraud detection using methods from utils
# Forward high-risk results to another Kafka topic

import os
import sys

if '__file__' in globals():
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
else:
    # For notebooks or interactive mode
    PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), '..'))

sys.path.insert(0, PROJECT_ROOT)
MODEL_DIR = os.path.join(PROJECT_ROOT, 'models')
ALERTS_CSV_PATH = os.path.join(PROJECT_ROOT, 'high_risk_alerts.csv')

from confluent_kafka import Consumer, KafkaException, Producer
import json
import csv
import pandas as pd
import time
import joblib
from utils.inference import preprocess_data, predict_fraud, predict_single

class FraudDetectionConsumer:
    """
    Consumes transactions from Kafka, runs feature engineering, preprocess for saved model, and makes predictions.
    """

    def __init__(
            self,
            bootstrap_servers: str = 'localhost:9092',
            topic: str = 'transactions',
            group_id: str = 'fraud-detection-group',
            model_path: str = None,
            feature_order_path: str = None
    ):
        # Kafka consumer setup
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe([topic])

        # Kafka producer setup for alerts
        self.alert_topic = 'high-risk-transactions'
        self.alert_producer = Producer({'bootstrap.servers': bootstrap_servers})

        # Resolve model paths dynamically
        if model_path is None:
            model_path = os.path.join(MODEL_DIR, 'fraud_detection_model.pkl')
        if feature_order_path is None:
            feature_order_path = os.path.join(MODEL_DIR, 'feature_order.pkl')

        # ML model + feature order
        self.model = joblib.load(model_path)
        self.feature_order = joblib.load(feature_order_path)

        print(f"Subscribed to topic '{topic}'")
        print("Loaded model and feature order.")

    def run(self, batch_size: int = 1):
        """
        Continuously poll Kafka, batch 'batch_size' messages, and run them through fraud detection.
        """
        buffer = []
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Parse incoming JSON into python dict
                tx = json.loads(msg.value().decode('utf-8'))

                # Ensure tx is flat and clean (drop fields not needed in inference)
                keys_to_keep = ['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg',
                                'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest']
                clean_tx = {k: tx[k] for k in keys_to_keep if k in tx}
                buffer.append(clean_tx)

                # Process `batch_size` items once available
                if len(buffer) >= batch_size:
                    self._process_batch(buffer)
                    buffer.clear()

        except KeyboardInterrupt:
            print("Interrupted by user, shutting down...")
        finally:
            self.consumer.close()
            print("Consumer closed.")

    def _process_batch(self, transactions: list[dict]):
        frauds = 0
        for tx in transactions:
            result = predict_single(tx, model=self.model, feature_order=self.feature_order)
            row = result.iloc[0]

            tx['fraud_probability'] = float(row['fraud_probability'])
            tx['fraud_prediction'] = int(row['fraud_prediction'])

            log = f"{tx['nameOrig']} → {tx['nameDest']} | {tx['type']} | ${tx['amount']:.2f} | {tx['fraud_probability']*100:.1f}%"

            if tx['fraud_prediction'] == 1:
                print(f"🚨 FRAUD: {log}")
                frauds += 1
            elif tx['fraud_probability'] > 0.3:
                print(f"⚠️ HIGH-RISK: {log}")
            else:
                print(f"✅ LEGIT: {log}")

            # Forward to alert topic in kafka_high_risk_consumer.py if flagged
            if tx['fraud_prediction'] == 1 or tx['fraud_probability'] > 0.3:
                self.alert_producer.produce(
                    topic=self.alert_topic,
                    key=tx['nameOrig'],
                    value=json.dumps(tx)
                )
                self.alert_producer.flush()

                with open(ALERTS_CSV_PATH, mode='a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=[
                        'step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg',
                        'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest',
                        'fraud_probability', 'fraud_prediction'
                        ])
                    writer.writerow(tx)

        print(f"Processed {len(transactions)} transactions. Flagged {frauds} frauds.\n")

    def stream_predictions(self, timeout=1.0):
        try:
            while True:
                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    yield "No new message"
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                tx = json.loads(msg.value().decode('utf-8'))

                keys_to_keep = ['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg',
                                'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest']
                clean_tx = {k: tx[k] for k in keys_to_keep if k in tx}

                result = predict_single(clean_tx, model=self.model, feature_order=self.feature_order)
                row = result.iloc[0]

                prob = float(row['fraud_probability'])
                pred = int(row['fraud_prediction'])

                if pred == 1:
                    label = "🚨 FRAUD"
                elif prob > 0.3:
                    label = "⚠️ HIGH RISK"
                else:
                    label = "✅ LEGIT"

                summary = (f"{label} | {clean_tx['type']} | "
                           f"{clean_tx['nameOrig']} → {clean_tx['nameDest']} | "
                           f"${clean_tx['amount']:.2f} | {prob * 100:.1f}% Fraud Probability")

                yield summary
        except KeyboardInterrupt:
            self.consumer.close()

if __name__ == "__main__":
    # Adjust batch_size to control how many messages you aggregate
    consumer = FraudDetectionConsumer()
    consumer.run(batch_size=5)