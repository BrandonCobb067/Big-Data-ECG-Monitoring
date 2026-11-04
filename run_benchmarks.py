import subprocess
import time
import socket
import re
import os
import threading
import select
from pymongo import MongoClient

SPARK_PORT = 9999
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "ecg_pipeline"
COLLECTION_NAME = "processed_signals"
DATASETS = ["chapman", "mitbih", "peco", "transfer", "ptbxl"]
PIPELINES = ["A", "B"]

class TCPBroker(threading.Thread):
    """
    Acts as the missing TCP server bridging Spark (client) and the Edge Simulators (clients).
    """
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        self.stop_event = threading.Event()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        # High backlog to accept 500+ concurrent edge device connections rapidly
        self.server.listen(1000) 
        
    def run(self):
        inputs = [self.server]
        spark_client = None
        
        while not self.stop_event.is_set():
            readable, _, _ = select.select(inputs, [], [], 1.0)
            for s in readable:
                if s is self.server:
                    client, _ = s.accept()
                    inputs.append(client)
                    # Because Spark starts 10 seconds before the simulator, 
                    # it will reliably be the first connection we accept.
                    if spark_client is None:
                        spark_client = client
                else:
                    try:
                        data = s.recv(524288)
                        if data:
                            # Route payloads from edge nodes exclusively to Spark
                            if spark_client and s is not spark_client:
                                try:
                                    spark_client.sendall(data)
                                except Exception:
                                    pass
                        else:
                            inputs.remove(s)
                            if s is spark_client:
                                spark_client = None
                            s.close()
                    except Exception:
                        inputs.remove(s)
                        if s is spark_client:
                            spark_client = None
                        s.close()
        
        for s in inputs:
            s.close()
            
    def stop(self):
        self.stop_event.set()
        self.join()

def wait_for_port_state(port: int, host: str = 'localhost', expect_open: bool = True, timeout: int = 30) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            result = sock.connect_ex((host, port))
            is_open = (result == 0)
            if expect_open and is_open:
                return True
            if not expect_open and not is_open:
                return True
        time.sleep(1)
    return False

def wipe_mongodb():
    print("Wiping MongoDB collection to ensure clean storage metrics...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db[COLLECTION_NAME].drop()
    client.close()

def main():
    print("="*80)
    print("STARTING ECG PIPELINE AUTOMATED BENCHMARK SUITE")
    print("="*80)

    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    for dataset in DATASETS:
        for pipeline in PIPELINES:
            print(f"\n{'-'*80}")
            print(f"Testing Dataset: {dataset.upper()} | Pipeline: {pipeline}")
            print(f"{'-'*80}")

            wipe_mongodb()

            # 1. Start the TCP Broker (The Server)
            print(f"Starting TCP Broker on port {SPARK_PORT}...")
            broker = TCPBroker("127.0.0.1", SPARK_PORT)
            broker.start()
            
            if not wait_for_port_state(SPARK_PORT, expect_open=True, timeout=10):
                print("ERROR: Broker failed to bind to port. Skipping...")
                broker.stop()
                continue

            # 2. Start Spark (Client 1)
            print("Starting Spark Structured Streaming background job...")
            spark_process = subprocess.Popen(["python3", "src/processing/spark_streaming.py"], env=env)
            
            # Allow the JVM time to initialize and establish its connection to the broker
            time.sleep(10)

            # 3. Execute Stream Simulator (Clients 2-501)
            print("Spinning up Async Stream Simulator...")
            simulator_cmd = ["python3", "src/stream_simulator.py", "--dataset", dataset, "--pipeline", pipeline]
            sim_result = subprocess.run(simulator_cmd, capture_output=True, text=True, env=env)

            # 4. Extract Metrics
            match = re.search(r'(BENCHMARK_RESULT\|.*)', sim_result.stdout)
            if match:
                benchmark_data = match.group(1)
                print(f"Simulator complete. Emitted Metrics: {benchmark_data}")
                
                print("Running lightweight KPI collector...")
                subprocess.run(["python3", "src/metrics_collector.py", "--benchmark_data", benchmark_data], env=env)
            else:
                print("ERROR: Simulator did not output BENCHMARK_RESULT. Stderr follows:")
                print(sim_result.stderr)

            # 5. Cleanup
            print("Shutting down Spark process and TCP Broker...")
            spark_process.terminate()
            try:
                spark_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                spark_process.kill()
                
            broker.stop()

            if not wait_for_port_state(SPARK_PORT, expect_open=False, timeout=30):
                print("WARNING: Port did not close gracefully.")

    print("\n" + "="*80)
    print("BENCHMARKING COMPLETE. Metrics exported to results/benchmark_metrics.csv")
    print("="*80)

if __name__ == "__main__":
    main()