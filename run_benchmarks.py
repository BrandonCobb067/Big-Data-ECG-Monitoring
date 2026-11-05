import subprocess
import time
import re
import os
import shutil
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "ecg_pipeline"
COLLECTION_NAME = "processed_signals"
DATASETS = ["chapman", "mitbih", "peco", "transfer", "ptbxl"]
PIPELINES = ["A", "B"]

# Map to your actual Kafka extraction path
KAFKA_BIN = os.path.expanduser("~/kafka_2.13-4.4.0/bin/kafka-topics.sh")

def wipe_state():
    print("Wiping MongoDB collection...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db[COLLECTION_NAME].drop()
    client.close()

    print("Wiping Spark checkpoints...")
    checkpoint_dir = "./spark_checkpoints"
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)

    print("Wiping Kafka topic...")
    # Delete the topic
    subprocess.run([
        KAFKA_BIN, 
        "--delete", 
        "--topic", "ecg_topic", 
        "--bootstrap-server", "localhost:9092"
    ], capture_output=True)
    
    print("Waiting for Kafka deletion to resolve...")
    time.sleep(5)

    print("Pre-creating fresh Kafka topic...")
    # Explicitly recreate the topic before Spark starts
    subprocess.run([
        KAFKA_BIN,
        "--create",
        "--topic", "ecg_topic",
        "--partitions", "1",
        "--replication-factor", "1",
        "--bootstrap-server", "localhost:9092"
    ], capture_output=True)

def main():
    print("="*80)
    print("STARTING ECG PIPELINE AUTOMATED BENCHMARK SUITE (KAFKA EDITION)")
    print("="*80)

    results_dir = "./results"
    if os.path.exists(results_dir):
        print(f"Wiping existing {results_dir} directory for a fresh run...")
        shutil.rmtree(results_dir)
    # Recreate the empty folder so metrics_collector doesn't error out looking for it
    os.makedirs(results_dir, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    for dataset in DATASETS:
        for pipeline in PIPELINES:
            print(f"\n{'-'*80}")
            print(f"Testing Dataset: {dataset.upper()} | Pipeline: {pipeline}")
            print(f"{'-'*80}")

            wipe_state()

            print("Starting Spark Structured Streaming background job...")
            spark_cmd = [
                "spark-submit",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0",
                "src/processing/spark_streaming.py" 
            ]
            spark_process = subprocess.Popen(spark_cmd, env=env)
            
            time.sleep(15)

            print("Spinning up Async Stream Simulator...")
            simulator_cmd = ["python", "src/stream_simulator.py", "--dataset", dataset, "--pipeline", pipeline]
            sim_result = subprocess.run(simulator_cmd, capture_output=True, text=True, env=env)

            match = re.search(r'(BENCHMARK_RESULT\|.*)', sim_result.stdout)
            if match:
                benchmark_data = match.group(1)
                print(f"Simulator complete. Emitted Metrics: {benchmark_data}")
                subprocess.run(["python", "src/metrics_collector.py", "--benchmark_data", benchmark_data], env=env)
            else:
                print("ERROR: Simulator did not output BENCHMARK_RESULT.")
                print(sim_result.stderr)

            # --- NEW KAFKA DRAIN LOGIC ---
            print("\nWaiting for Spark to drain Kafka buffer and write to MongoDB...")
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            
            previous_count = -1
            stable_checks = 0
            
            # Require 3 consecutive checks with the exact same record count before assuming Spark is finished
            while stable_checks < 3:
                # Failsafe: Break if Spark crashes in the background
                if spark_process.poll() is not None:
                    print("\nERROR: Spark process died unexpectedly.")
                    break
                
                time.sleep(5)
                current_count = db[COLLECTION_NAME].count_documents({})
                print(f" -> MongoDB Records Stored: {current_count}")
                
                if current_count > 0 and current_count == previous_count:
                    stable_checks += 1
                elif current_count == 0 and previous_count == 0:
                    # Spark is still initializing or processing the very first batch
                    pass 
                else:
                    stable_checks = 0
                    
                previous_count = current_count
                
            client.close()
            print("Spark processing complete!")
            # -----------------------------

            print("Shutting down Spark process...")
            spark_process.terminate()
            try:
                spark_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                spark_process.kill()

    print("\n" + "="*80)
    print("BENCHMARKING COMPLETE.")
    print("="*80)

if __name__ == "__main__":
    main()