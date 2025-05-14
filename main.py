import pandas as pd
from utils import predict_single

sample_transaction = {
    'step': 333,
    'type': 'CASH-OUT',
    'amount': 184429.71,
    'nameOrig': 'C1231006815',
    'oldbalanceOrg': 206500.71,
    'newbalanceOrig': 22071.0,
    'nameDest': 'C1231006815',
    'oldbalanceDest': 0,
    'newbalanceDest': 0,
    'isFraud': 0,
    'isFlaggedFraud': 0
}

result = predict_single(sample_transaction)

print("\nPrediction Output:")
print(result[['amount', 'nameOrig', 'nameDest', 'fraud_probability', 'fraud_prediction']])