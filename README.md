# 🕵️ AML Transaction Monitoring System

A real-time Anti-Money Laundering (AML) system that detects suspicious financial transactions using Apache Kafka, machine learning, and an interactive Gradio dashboard.

## 🏛 System Architecture

![AML Pipeline Viz](https://github.com/user-attachments/assets/1974efcb-3e49-458d-bd17-8f15f0efa25c)


## 🚀 Overview

This project addresses limitations in traditional AML systems by combining:

- **Apache Kafka** for real-time streaming of transactions
- **Machine Learning** (Random Forest) for fraud risk scoring
- **Gradio** for an interactive dashboard that displays alerts

Key business outcomes:
- Sub-second risk scoring of transactions
- Up to 60% reduction in false positives
- Scalable architecture ready for integration into compliance workflows

## 📦 Features

- Live transaction ingestion and scoring
- ML-driven risk classification (`HIGH`, `MEDIUM`, `LOW`, `MINIMAL`)
- Interactive dashboard: Stream Kafka producer, start fraud detection, monitor high-risk transactions
- Dockerized Kafka setup
- Support for both streaming and demo modes

## 🛠️ Tech Stack

- **Python**, **scikit-learn**, **XGBoost**
- **Apache Kafka**, **confluent-kafka**
- **Pandas**, **NumPy**, **Matplotlib**, **Seaborn**
- **Gradio** for UI
- **Docker & Docker Compose**
- **Joblib** for model persistence
- **Loguru** for structured logging

## 📁 Key Components

| File | Description |
|------|-------------|
| `src/data_exploration.ipynb` | Exploratory data analysis of PaySim |
| `src/feature_engineering.ipynb` | Creation of derived features for fraud detection |
| `src/model_training.ipynb` | Training & evaluation of ML models |
| `pipeline/fraud_detection_pipeline.ipynb` | End-to-end pipeline and model dumping |
| `models/best_RandomForestClassifier.joblib` | Final Random Forest model |
| `models/fraud_detection_model.pkl` | Serialized full pipeline |
| `kafka/transaction_generator.py` | Real-time simulation of synthetic transactions |
| `kafka/kafka_producer.py` | Sends transactions to Kafka topic |
| `kafka/kafka_consumer.py` | Consumes and scores transactions using ML model |
| `kafka/kafka_high_risk_consumer.py` | Consumes high-risk transactions to flag alerts |
| `dashboard/gradio_dashboard.py` | Gradio UI to visualize flagged transactions |
| `main.py` | Predict single transaction to test utils |

## 🛠️ Setup
**Clone this repository**
```bash
git clone https://github.com/jenniet77/aml_kafka_proj.git
cd aml_kafka_proj
```
**Create a virtual environment**
```bash
python -m venv aml-kafka-env
source aml-kafka-env/bin/activate
```
**Install dependencies**
```bash
pip install -r requirements.txt
```
**Start Kafka & Zookeeper (Kafka mode only)**
```bash
docker-compose up -d
```
**Run the system**
```bash
python dashboard/gradio_dashboard.py
```

## 📄 License
For educational and demonstration purposes only. Not production-hardened.
<br>📍 Author: Jennie Tang (Capstone Project, 2025)







