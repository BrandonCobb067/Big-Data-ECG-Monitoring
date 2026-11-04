"""
Centralized configurations for the ECG Pipeline Simulator
"""

# SIMULATION & EDGE PARAMETERS
# Number of concurrent virtual devices to simulate in the asyncio loop
SIMULATED_DEVICES_COUNT = 50

# Standardized Length of the ECG segments being yielded
CHUNK_DURATION_SECONDS = 10

#  The asyncio.sleep() duration required to mimic the processing time of a low-power Cortex-M microcontroller for pipeline B
COMPUTE_DELAY_SECONDS = 0.05



# SPARK STREAMING NETWORK SETTINGS
# IP addresses and port for the Spark Structured Streaming TCP receiver
SPARK_RECEIVER_IP = "127.0.0.1"
SPARK_RECEIVER_PORT = 9999



# DATABASE SETTINGS
# Connection string for the MongoDB instance
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "ecg_pipeline"
MONGO_COLLECTION_NAME = "processed_signals"