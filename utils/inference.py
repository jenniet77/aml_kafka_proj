import pandas as pd
import numpy as np
import joblib
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MODEL_DIR = os.path.join(PROJECT_ROOT, 'models')

def engineer_features(data):
    df = data.copy()
    df = df.drop(columns=['transaction_id'], errors='ignore')

    # Extract account types
    df['origAccountType'] = data['nameOrig'].str[0]
    df['destAccountType'] = data['nameDest'].str[0]

    # Flag for Customer-to-Customer transactions
    df['C_to_C'] = ((df['origAccountType'] == 'C') & (df['destAccountType'] == 'C')).astype(int)

    # Log-transform amount to reduce skewness
    df['logAmount'] = np.log1p(data['amount'])

    # Balance features
    df['oldbalanceDiff'] = data['oldbalanceOrg'] - data['oldbalanceDest']
    df['newbalanceDiff'] = data['newbalanceOrig'] - data['newbalanceDest']
    df['balanceChangeOrig'] = data['newbalanceOrig'] - data['oldbalanceOrg']
    df['balanceChangeDest'] = data['newbalanceDest'] - data['oldbalanceDest']

    # Check if transaction zeroes out the origin account
    df['isOriginZeroed'] = (data['oldbalanceOrg'] > 0) & (data['newbalanceOrig'] == 0)

    # Ratio of transaction amount to old balance
    # To avoid division by zero, we'll add a small epsilon
    epsilon = 1e-10
    df['amountToOldBalanceRatio'] = data['amount'] / (data['oldbalanceOrg'] + epsilon)

    # Is the transaction amount suspiciously close to the total balance?
    df['isAmountCloseToBalance'] = (
        abs(data['amount'] - data['oldbalanceOrg']) / (data['oldbalanceOrg'] + epsilon) < 0.05
    ) & (data['oldbalanceOrg'] > 0)

    # Error/oddity detection using np.isclose for balance check
    df['errorBalanceOrig'] = ~np.isclose(data['newbalanceOrig'] + data['amount'], data['oldbalanceOrg'])
    df['errorBalanceDest'] = ~np.isclose(data['oldbalanceDest'] + data['amount'], data['newbalanceDest'])

    # One-hot encode categorical features
    # Only encode columns that actually exist
    categorical_cols = [col for col in ['type', 'origAccountType', 'destAccountType'] if col in df.columns]
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    return df

def preprocess_data(df, feature_order=None):
    if feature_order is None:
        feature_order = joblib.load(os.path.join(MODEL_DIR, 'feature_order.pkl'))

    # Apply the same feature engineering as before
    processed_df = engineer_features(df)

    # Ensure all expected columns are present
    missing_cols = [col for col in feature_order if col not in processed_df.columns]
    for col in missing_cols:
        processed_df[col] = 0

    # Reorder to match training set
    processed_df = processed_df[feature_order]

    # print("⚙️ Columns in engineered data:", processed_df.columns.tolist())
    # print("⚙️ Columns expected by model:", feature_order)

    return processed_df

def predict_fraud(transaction_data, model=None, feature_order=None):
    """
    Make fraud predictions on new transaction data.

    Parameters:
    -----------
    transaction_data : pandas DataFrame
        New transaction data with the same format as the original dataset

    Returns:
    --------
    DataFrame with original data plus fraud predictions and probabilities
    """
    # Preprocess the data
    processed_data = preprocess_data(transaction_data, feature_order)

    # Load the model
    if model is None:
        model = joblib.load(os.path.join(MODEL_DIR, 'fraud_detection_model.pkl'))

    # Make predictions
    fraud_proba = model.predict_proba(processed_data)[:, 1]
    fraud_pred = model.predict(processed_data)

    # Add predictions to the original data
    result = transaction_data.copy()
    result['fraud_probability'] = fraud_proba
    result['fraud_prediction'] = fraud_pred

    return result

def predict_single(tx_dict, model=None, feature_order=None):
    if feature_order is None:
        feature_order = joblib.load(os.path.join(MODEL_DIR, 'feature_order.pkl'))
    df = pd.DataFrame([tx_dict])
    return predict_fraud(df, model=model, feature_order=feature_order)