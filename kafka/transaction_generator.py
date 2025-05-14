import random
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

class TransactionGenerator:
    """
    Generates synthetic financial transactions similar to the PaySim dataset
    to feed into Apache Kafka for real-time fraud detection.
    """

    def __init__(self, sample_data=None, fraud_ratio=0.5):
        """
        Initialize the transaction generator.

        Args:
            sample_data (pd.DataFrame, optional): Sample data to base generation on
            fraud_ratio (float): Ratio of fraudulent transactions to generate
        """
        self.transaction_types = ['PAYMENT', 'TRANSFER', 'CASH_OUT', 'CASH_IN', 'DEBIT']
        self.type_weights = [0.4, 0.2, 0.2, 0.15, 0.05]  # Weights for random selection
        self.fraud_ratio = fraud_ratio
        self.sample_data = sample_data
        self.customer_ids = self._generate_customer_ids(1000)
        self.merchant_ids = self._generate_merchant_ids(200)
        self.step = 1  # Start at step 1
        self.current_time = datetime.now()

        # Initialize customer balances
        self.customer_balances = {
            customer: random.uniform(1000, 100000)
            for customer in self.customer_ids
        }

    def _generate_customer_ids(self, n):
        """Generate n unique customer IDs"""
        return [f'C{1000000000 + i}' for i in range(n)]

    def _generate_merchant_ids(self, n):
        """Generate n unique merchant IDs"""
        return [f'M{1000000000 + i}' for i in range(n)]

    def _random_amount(self, transaction_type):
        """Generate a realistic random amount based on transaction type"""
        if transaction_type == 'PAYMENT':
            return round(random.uniform(10, 5000), 2)
        elif transaction_type == 'TRANSFER':
            return round(random.uniform(10, 1000000), 2)
        elif transaction_type == 'CASH_OUT':
            return round(random.uniform(10, 500000), 2)
        elif transaction_type == 'CASH_IN':
            return round(random.uniform(10, 50000), 2)
        elif transaction_type == 'DEBIT':
            return round(random.uniform(10, 3000), 2)

    def _generate_fraud(self, transaction_type):
        """
        Determine if a transaction should be fraudulent based on type and desired ratio
        """
        # Only TRANSFER and CASH_OUT can be fraudulent in the original dataset
        # if transaction_type not in ['TRANSFER', 'CASH_OUT']:
        #     return 0
        #
        # # Generate fraud based on the desired ratio
        # if random.random() < self.fraud_ratio:
        #     return 1
        # return 0
        #
        # OPTIONAL Activate this fraud logic for testing only
        return 1 if transaction_type in ['TRANSFER', 'CASH_OUT'] and random.random() < self.fraud_ratio else 0

    def _update_balances(self, nameOrig, nameDest, amount, transaction_type, is_fraud):
        """Update customer balances based on transaction details"""
        # Get initial balances
        oldbalanceOrig = self.customer_balances.get(nameOrig, 0)

        # For merchants, we don't track balances in the original dataset
        if nameDest.startswith('M'):
            oldbalanceDest = 0
            # Merchants always receive the full amount
            newbalanceDest = 0
        else:
            oldbalanceDest = self.customer_balances.get(nameDest, 0)
            # Update destination balance for non-merchant
            if transaction_type in ['TRANSFER', 'CASH_IN']:
                self.customer_balances[nameDest] = oldbalanceDest + amount
            newbalanceDest = self.customer_balances.get(nameDest, 0)

        # Update origin balance
        if transaction_type in ['PAYMENT', 'TRANSFER', 'CASH_OUT']:
            # In fraud cases, sometimes the entire balance is taken
            if is_fraud and random.random() < 0.8:
                self.customer_balances[nameOrig] = 0
            else:
                self.customer_balances[nameOrig] = max(0, oldbalanceOrig - amount)

        elif transaction_type == 'CASH_IN':
            # Cash in increases the origin balance
            self.customer_balances[nameOrig] += amount

        newbalanceOrig = self.customer_balances.get(nameOrig, 0)

        return oldbalanceOrig, newbalanceOrig, oldbalanceDest, newbalanceDest

    def generate_transaction(self):
        """
        Generate a single random transaction

        Returns:
            dict: Transaction details as a dictionary
        """
        # Choose transaction type
        transaction_type = random.choices(self.transaction_types, weights=self.type_weights)[0]

        # Generate amount
        amount = self._random_amount(transaction_type)

        # Select customer for origin
        nameOrig = random.choice(self.customer_ids)

        # Determine if this will be a fraudulent transaction
        # is_fraud = self._generate_fraud(transaction_type)

        # OPTIONAL: Activate this to force fraud for every batch
        is_fraud = 1 if transaction_type in ['TRANSFER', 'CASH_OUT'] else 0

        # Set isFlaggedFraud for large transfers (over 200,000)
        is_flagged_fraud = 1 if transaction_type == 'TRANSFER' and amount > 200000 else 0

        # Select destination based on transaction type
        if transaction_type == 'PAYMENT':
            nameDest = random.choice(self.merchant_ids)
        else:
            # For non-payment transactions, destination is usually another customer
            nameDest = random.choice([c for c in self.customer_ids if c != nameOrig])

        # # Update balances
        # oldbalanceOrig, newbalanceOrig, oldbalanceDest, newbalanceDest = self._update_balances(
        #     nameOrig, nameDest, amount, transaction_type, is_fraud
        # )

        # OPTIONAL: Activate this for TRAINING-ALIGNED FRAUD PATTERN
        if transaction_type in ['TRANSFER', 'CASH_OUT'] and is_fraud:
            oldbalanceOrg = amount
            newbalanceOrig = 0.0
            oldbalanceDest = 0.0
            newbalanceDest = 0.0
        else:
            oldbalanceOrg = self.customer_balances.get(nameOrig, 0)
            if transaction_type in ['TRANSFER', 'CASH_OUT']:
                newbalanceOrig = max(0, oldbalanceOrg - amount)
            elif transaction_type == 'CASH_IN':
                newbalanceOrig = oldbalanceOrg + amount
            else:
                newbalanceOrig = oldbalanceOrg

            if nameDest.startswith('M'):
                oldbalanceDest = 0.0
                newbalanceDest = 0.0
            else:
                oldbalanceDest = self.customer_balances.get(nameDest, 0)
                if transaction_type in ['TRANSFER', 'CASH_IN']:
                    newbalanceDest = oldbalanceDest + amount
                else:
                    newbalanceDest = oldbalanceDest

        # Create transaction record
        transaction = {
            'transaction_id': f"tx_{self.step}_{random.randint(10000, 99999)}",
            'step': self.step,
            'type': transaction_type,
            'amount': amount,
            'nameOrig': nameOrig,
            'oldbalanceOrg': oldbalanceOrg,
            'newbalanceOrig': newbalanceOrig,
            'nameDest': nameDest,
            'oldbalanceDest': oldbalanceDest,
            'newbalanceDest': newbalanceDest,
            'isFraud': is_fraud,
            'isFlaggedFraud': is_flagged_fraud,
            'timestamp': self.current_time.isoformat()
        }

        return transaction

    def generate_batch(self, batch_size=100):
        """
        Generate a batch of transactions

        Args:
            batch_size (int): Number of transactions to generate

        Returns:
            list: List of transaction dictionaries
        """
        transactions = []

        for _ in range(batch_size):
            transaction = self.generate_transaction()
            transactions.append(transaction)

        # Increment step after batch (to simulate passage of time)
        self.step += 1
        self.current_time += timedelta(hours=1)

        return transactions


# if __name__ == "__main__":
#     # One-time test run
#     generator = TransactionGenerator(fraud_ratio=0.02)
#     test_batch = generator.generate_batch(5)  # generate 5 transactions only
#
#     print("Sample generated transactions:")
#     for tx in test_batch:
#         print(json.dumps(tx, indent=2))
