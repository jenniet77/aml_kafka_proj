import sys
import os
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

import gradio as gr
import pandas as pd
import subprocess
import threading
import atexit
import queue
from kafka.kafka_consumer import FraudDetectionConsumer

# === INIT ===
# Use a queue for thread-safe communication
log_queue = queue.Queue()
high_risk_queue = queue.Queue()

producer_process = None
consumer_process = None
high_risk_process = None
streaming_enabled = False  # Start as False
stream_thread = None


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
    global consumer_process, streaming_enabled
    if consumer_process is None:
        try:
            consumer_process = subprocess.Popen(['python', 'kafka/kafka_consumer.py'])
            streaming_enabled = True  # Enable streaming when consumer starts
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


# === STREAM FUNCTIONS ===
def mock_consumer_stream():
    """A mock consumer that produces logs for testing when real consumer fails"""
    import random
    import time

    transaction_types = ["PAYMENT", "CASH_IN", "CASH_OUT", "DEBIT", "TRANSFER"]
    source_accounts = ["1000000123", "1000000456", "1000000789", "1000000012", "1000000345"]
    dest_accounts = ["2000000123", "2000000456", "2000000789", "2000000012", "2000000345"]

    while streaming_enabled:
        time.sleep(2)  # Mock delay

        # Generate a random transaction
        trans_type = random.choice(transaction_types)
        source = random.choice(source_accounts)
        dest = random.choice(dest_accounts)
        amount = round(random.uniform(100, 10000), 2)

        # Determine if this is a fraud (10% chance)
        is_fraud = random.random() < 0.1
        probability = round(random.uniform(0.7, 0.99), 2) if is_fraud else round(random.uniform(0.01, 0.3), 2)

        if is_fraud:
            prefix = "🚨 FRAUD"
        elif probability > 0.5:
            prefix = "⚠️ HIGH RISK"
        else:
            prefix = "✅ LEGIT"

        # Format the message
        message = f"{prefix} | {trans_type} | {source} → {dest} | ${amount:.2f} | {probability * 100:.1f}% Fraud Probability"

        yield message


def try_get_consumer():
    """Try to get a working consumer, fall back to mock if needed"""
    try:
        consumer = FraudDetectionConsumer()
        return consumer.stream_predictions()
    except Exception as e:
        print(f"Error creating consumer, using mock: {e}")
        return mock_consumer_stream()


def stream_loop():
    """Main stream processing loop"""
    global streaming_enabled

    streaming_enabled = True
    live_log = []
    high_risk_rows = []

    try:
        for prediction in try_get_consumer():
            if not streaming_enabled:
                print("Streaming disabled, exiting stream loop")
                break

            # Process the prediction
            live_log.append(prediction)
            if len(live_log) > 10:
                live_log.pop(0)

            # Put in queue for UI thread to consume
            log_queue.put("\n".join(live_log))

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
                    # Update the high risk table
                    high_risk_queue.put(pd.DataFrame(high_risk_rows))
                except IndexError:
                    print(f"Error parsing: {prediction}")
    except Exception as e:
        print(f"Stream loop error: {e}")
    finally:
        print("Stream loop exited")


def update_stream():
    """Get the latest stream data from the queue"""
    try:
        # Non-blocking get
        return log_queue.get(block=False)
    except queue.Empty:
        return None


def update_table():
    """Get the latest high risk data from the queue"""
    try:
        # Non-blocking get
        return high_risk_queue.get(block=False)
    except queue.Empty:
        return None


def stop_all_processes():
    """Stop all processes and cleanup resources"""
    global producer_process, consumer_process, high_risk_process, streaming_enabled, stream_thread

    print("Stopping all processes...")
    streaming_enabled = False

    # Stop subprocesses
    processes = [producer_process, consumer_process, high_risk_process]
    for process in processes:
        if process is not None:
            try:
                process.terminate()
                process.wait(timeout=2)  # Wait for the process to terminate
            except Exception as e:
                print(f"Error terminating process: {e}")

    # Reset process variables
    producer_process, consumer_process, high_risk_process = None, None, None

    # Clear queues
    while not log_queue.empty():
        log_queue.get()
    while not high_risk_queue.empty():
        high_risk_queue.get()

    # Start with a clean message
    log_queue.put("All processes stopped. Start the producer and consumer to see logs.")
    high_risk_queue.put(pd.DataFrame(columns=["Type", "Origin", "Destination", "Amount", "Probability"]))

    return "All processes stopped."


def get_system_status():
    """Get the current system status"""
    return {
        "Producer running": producer_process is not None and producer_process.poll() is None,
        "Consumer running": consumer_process is not None and consumer_process.poll() is None,
        "High-risk viewer running": high_risk_process is not None and high_risk_process.poll() is None,
        "Streaming enabled": streaming_enabled,
        "Stream thread alive": stream_thread is not None and stream_thread.is_alive(),
        "Log queue size": log_queue.qsize(),
        "High risk queue size": high_risk_queue.qsize()
    }


def start_stream_thread():
    """Start the streaming thread if not already running"""
    global stream_thread, streaming_enabled

    if stream_thread is None or not stream_thread.is_alive():
        streaming_enabled = True
        stream_thread = threading.Thread(target=stream_loop, daemon=True)
        stream_thread.start()
        return "Stream thread started"
    return "Stream thread already running"


# === THEME ===
mono_theme = gr.themes.Monochrome()
# glass_theme = gr.themes.Soft(primary_hue="blue", neutral_hue="gray")

# === UI ===
with gr.Blocks(theme=mono_theme, title="Real-Time AML Monitoring System") as demo:
    gr.Markdown("## AML Transaction Fraud Monitoring Dashboard")

    with gr.Row():
        producer_btn = gr.Button("Start Kafka Producer")
        consumer_btn = gr.Button("Start Fraud Detection")
        viewer_btn = gr.Button("View High-Risk Alerts")
        stop_btn = gr.Button("Stop Stream")
        stop_all_btn = gr.Button("Stop All Processes")

    status_output = gr.Textbox(label="Status", interactive=False)

    with gr.Row():
        live_stream = gr.Textbox(lines=10, label="Live Prediction Stream", interactive=False, every=1)
        high_risk_table = gr.Dataframe(
            headers=["Type", "Origin", "Destination", "Amount", "Probability"],
            label="⚠️ High-Risk Transactions",
            interactive=False,
            row_count=5,
            every=1
        )

    # Add debug section
    with gr.Accordion("Debug Information", open=False):
        debug_btn = gr.Button("Show Debug Info")
        restart_thread_btn = gr.Button("Restart Stream Thread")
        debug_output = gr.JSON(label="System Status")

    # Button interactions
    producer_btn.click(start_producer, outputs=status_output)
    consumer_btn.click(start_consumer, outputs=status_output)
    viewer_btn.click(start_high_risk_viewer, outputs=status_output)
    stop_btn.click(stop_streaming, outputs=status_output)
    stop_all_btn.click(stop_all_processes, outputs=status_output)
    debug_btn.click(get_system_status, outputs=debug_output)
    restart_thread_btn.click(start_stream_thread, outputs=status_output)

    # UI component update settings
    demo.load(fn=lambda: "Dashboard ready. Start producer and consumer.", outputs=status_output)

    # Connect the periodic update functions to the UI components
    live_stream.update(update_stream, every=1)
    high_risk_table.update(update_table, every=1)

# Initialize empty output
log_queue.put("Dashboard initialized. Start the producer and consumer to see logs.")
high_risk_queue.put(pd.DataFrame(columns=["Type", "Origin", "Destination", "Amount", "Probability"]))

# Register cleanup
atexit.register(stop_all_processes)

# Start with a clean system
stop_all_processes()

if __name__ == "__main__":
    try:
        # Start the stream thread
        start_stream_thread()

        # Launch the Gradio interface
        demo.queue().launch()
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        stop_all_processes()
    except Exception as e:
        print(f"Error launching app: {e}")
        stop_all_processes()