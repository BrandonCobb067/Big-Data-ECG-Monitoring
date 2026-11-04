"""
Lightweight script to capture benchmarking output, query MongoDB storage footprint,
and append results to a CSV file.
"""

import argparse
import csv
import os
from pymongo import MongoClient
import src.config as config

def get_mongo_storage_bytes() -> int:
    """Queries MongoDB for the exact byte size of the processed_signals collection."""
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB_NAME]
    
    # collStats provides deep metrics on the collection's actual disk footprint
    stats = db.command("collStats", config.MONGO_COLLECTION_NAME)
    storage_size = stats.get("storageSize", 0)
    
    client.close()
    return storage_size

def write_to_csv(dataset: str, pipeline: str, chunks: int, tx_bytes: int, elapsed_time: float, storage_bytes: int):
    """Appends the run metrics to the permanent CSV file."""
    os.makedirs("results", exist_ok=True)
    csv_file = "results/benchmark_metrics.csv"
    
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        # Write header if the file is brand new
        if not file_exists:
            writer.writerow([
                "Dataset", "Pipeline", "Total_Chunks", 
                "Transmitted_Bytes", "Storage_Bytes", "Latency_Seconds"
            ])
            
        writer.writerow([dataset, pipeline, chunks, tx_bytes, storage_bytes, elapsed_time])
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect and save pipeline metrics.")
    parser.add_argument('--benchmark_data', type=str, required=True,
                        help="Delimited string from stream_simulator.py")
    
    args = parser.parse_args()
    
    # Parse the delimited string: BENCHMARK_RESULT|dataset_name|pipeline_mode|total_chunks|total_bytes|elapsed_time
    try:
        parts = args.benchmark_data.split('|')
        if len(parts) != 6 or parts[0] != "BENCHMARK_RESULT":
            raise ValueError("Malformed benchmark data string.")
            
        dataset_name = parts[1]
        pipeline_mode = parts[2]
        total_chunks = int(parts[3])
        transmitted_bytes = int(parts[4])
        elapsed_time = float(parts[5])
        
        # Query the database to see how much space the data actually required on disk
        storage_bytes = get_mongo_storage_bytes()
        
        # Save to CSV
        write_to_csv(dataset_name, pipeline_mode, total_chunks, transmitted_bytes, elapsed_time, storage_bytes)
        
        print(f"Successfully logged metrics for {dataset_name.upper()} Pipeline {pipeline_mode}.")
        print(f" -> Transmitted: {transmitted_bytes} bytes | Stored: {storage_bytes} bytes | Time: {elapsed_time:.2f}s")
        
    except Exception as e:
        print(f"Error parsing metrics: {e}")