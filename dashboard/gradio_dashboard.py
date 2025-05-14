import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

import gradio as gr
import pandas as pd
import subprocess
import threading
import atexit
from kafka.kafka_consumer import FraudDetectionConsumer

# === INIT ===
try:
    consumer = FraudDetectionConsumer()
except Exception as e:
    print(f"Error initializing consumer: {e}")
    consumer = None

producer_process = None
consumer_process = None
high_risk_process = None
streaming_enabled = True
live_log = []
high_risk_rows = []

# Add a lock for thread safety
data_lock = threading.Lock()

# === PROCESS START/STOP ===
def start_producer():
    global producer_process
    if producer_process is None:
        try:
            producer_process = subprocess.Popen(['python', 'kafka/kafka_producer.py'])
            return "Producer started."
        except Exception as e:
            return f"Error starting producer: {e}"
    return "Producer already running..."

def start_consumer():
    global consumer_process
    if consumer_process is None:
        try:
            consumer_process = subprocess.Popen(['python', 'kafka/kafka_consumer.py'])
            return "Consumer started."
        except Exception as e:
            return f"Error starting consumer: {e}"
    return "Consumer already running..."

def start_high_risk_viewer():
    global high_risk_process
    if high_risk_process is None:
        try:
            high_risk_process = subprocess.Popen(['python', 'kafka/kafka_high_risk_consumer.py'])
            return "High-risk viewer started."
        except Exception as e:
            return f"Error starting high-risk viewer: {e}"
    return "High-risk viewer already running..."

def stop_streaming():
    global streaming_enabled
    streaming_enabled = False
    return "Live stream stopped."

# === STREAM + TABLE UPDATER ===
def stream_loop():
    global streaming_enabled, live_log, high_risk_rows, consumer

    if consumer is None:
        try:
            consumer = FraudDetectionConsumer()
        except Exception as e:
            print(f"Failed to initialize consumer in stream_loop: {e}")
            return

    streaming_enabled = True
    try:
        for prediction in consumer.stream_predictions():
            if not streaming_enabled:
                break

            with data_lock:
                live_log.append(prediction)
                if len(live_log) > 10:
                    live_log.pop(0)

                parts = prediction.split(" | ")
                if parts[0] in ["🚨 FRAUD", "⚠️ HIGH RISK"]:
                    try:
                        row = {
                            "Type": parts[1],
                            "Origin": parts[2].split(" → ")[0],
                            "Destination": parts[2].split(" → ")[1],
                            "Amount": parts[3].replace("$", ""),
                            "Probability": parts[4].replace("% Fraud Probability", "")
                        }
                        high_risk_rows.append(row)
                        high_risk_rows = high_risk_rows[-20:]
                    except IndexError:
                        print(f"Error parsing: {prediction}")
    except Exception as e:
        print(f"Error in stream: {e}")

def update_table():
    with data_lock:
        if high_risk_rows:
            df = pd.DataFrame(high_risk_rows)
            return df
        return pd.DataFrame(columns=["Type", "Origin", "Destination", "Amount", "Probability"])

def update_stream():
    with data_lock:
        return "\n".join(live_log) if live_log else ""


def stop_all_processes():
    global producer_process, consumer_process, high_risk_process, streaming_enabled
    streaming_enabled = False

    processes = [producer_process, consumer_process, high_risk_process]

    for process in processes:
        if process is not None:
            try:
                process.terminate()
            except Exception as e:
                print(f"Error terminating process: {e}")

    # After iteration, set all process variables to None
    producer_process, consumer_process, high_risk_process = None, None, None

    return "All processes stopped."

def is_process_running(process):
    return process is not None and process.poll() is None

def get_system_status():
    with data_lock:
        return {
            "Producer running": is_process_running(producer_process),
            "Consumer running": is_process_running(consumer_process),
            "High-risk viewer running": is_process_running(high_risk_process),
            "Streaming enabled": streaming_enabled,
            "Live log entries": len(live_log),
            "High-risk entries": len(high_risk_rows)
        }

# === THEME ===
glass_theme = gr.themes.Soft(primary_hue="blue", neutral_hue="gray")

# === UI ===
with gr.Blocks(theme=glass_theme, title="Real-Time AML Monitoring System") as demo:
    gr.Markdown("## AML Transaction Fraud Monitoring Dashboard")

    with gr.Row():
        producer_btn = gr.Button("Start Kafka Producer")
        consumer_btn = gr.Button("Start Fraud Detection")
        viewer_btn = gr.Button("View High-Risk Alerts")
        stop_btn = gr.Button("Stop Stream")
        stop_all_btn = gr.Button("Stop All Processes")

    status_output = gr.Textbox(label="Status", interactive=False)

    with gr.Row():
        live_stream = gr.Textbox(lines=10, label="Live Prediction Stream", interactive=False)
        high_risk_table = gr.Dataframe(
            headers=["Type", "Origin", "Destination", "Amount", "Probability"],
            label="⚠️ High-Risk Transactions",
            interactive=False,
            row_count=5
        )

    # Add debug section
    with gr.Accordion("Debug Information", open=False):
        debug_btn = gr.Button("Show Debug Info")
        debug_output = gr.JSON(label="System Status")

    # Button interactions
    producer_btn.click(start_producer, outputs=status_output)
    consumer_btn.click(start_consumer, outputs=status_output)
    viewer_btn.click(start_high_risk_viewer, outputs=status_output)
    stop_btn.click(stop_streaming, outputs=status_output)
    stop_all_btn.click(stop_all_processes, outputs=status_output)
    debug_btn.click(get_system_status, outputs=debug_output)

    # Live stream + table refresh bindings
    demo.load(update_stream, None, live_stream, every=2)
    demo.load(update_table, None, high_risk_table, every=5)

# Register cleanup
atexit.register(stop_all_processes)

# == THREADING FOR LIVE STREAM AND LOG ==
# Start streaming thread
stream_thread = threading.Thread(target=stream_loop, daemon=True)
stream_thread.start()
# threading.Thread(target=stream_loop, daemon=True).start()

if __name__ == "__main__":
    try:
        demo.launch()
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        stop_all_processes()
    except Exception as e:
        print(f"Error launching app: {e}")
        stop_all_processes()