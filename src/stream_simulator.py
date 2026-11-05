"""
Asynchronous orchestrator to simulate concurrent vital edge nodes transmitting ECG data
"""

import asyncio
import json
import time
from aiokafka import AIOKafkaProducer
from concurrent.futures import ThreadPoolExecutor
import numpy as np

import src.config as config
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.edge.edge_processor import process_edge_chunk

#Shared dictionary to track metrics across asynchronous coroutines
benchmarks = {
    "total_bytes": 0,
    "total_chunks": 0
}

class NumpyEncoder(json.JSONEncoder):
    """Custom encoder to serialize NumPy arrays to JSON."""
    def default (self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.float16):
            return float(obj)
        return super().default(obj)

async def virtual_edge_node(node_id: int, queue: asyncio.Queue, pipeline_mode: str, producer: AIOKafkaProducer):
    """
    Simulates a single concurrent IoT device processing and transmitting data via Kafka
    """
    
    try:
        while True:
            # Pull the next 10-second chunk from the thread-pool bridge
            ecg_record: ECGRecord = await queue.get()

            if ecg_record is None:
                queue.task_done()
                break

            payload = {
                "node_id": node_id,
                "pipeline_mode": pipeline_mode,
                "original_fs":ecg_record.sample_rate,
                "data": None
            }

            if pipeline_mode == "A":
                # Pipeline A: Transmit raw arrays immediately
                payload["data"] = ecg_record.samples
                payload["transmitted_fs"] = ecg_record.sample_rate

            elif pipeline_mode == "B":
                # Pipeline B: Route through edge_processor.py downsampling and delay
                target_fs = 250
                processed_samples = await process_edge_chunk(
                    ecg_samples=ecg_record.samples,
                    original_fs=ecg_record.sample_rate,
                    target_fs=target_fs
                )
                payload["data"] = processed_samples
                payload["transmitted_fs"] = target_fs

            # Serialize and push payload over raw TCP socket to Spark
            serialized_data = json.dumps(payload, cls=NumpyEncoder)
            encoded_data = serialized_data.encode('utf-8')

            benchmarks["total_bytes"] += len(encoded_data)
            benchmarks["total_chunks"] += 1
            
            await producer.send_and_wait("ecg_topic", encoded_data)

            queue.task_done()

    except Exception as e:
        print(f"Node {node_id} disconnected: {e}")

def blocking_reader_bridge(reader_instance: BaseReader, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    """
    Runs in a background thread to prevent zip-file I/O from blocking the async event loop
    """
    try:
        for chunk in reader_instance.yield_chunks():
            # .result() blocks the thread if the async queue is full, preventing memory leaks
            future = asyncio.run_coroutine_threadsafe(queue.put(chunk), loop)
            future.result() 
    except Exception as e:
        print(f"\nCRITICAL READER ERROR: {e}")
    finally:
        # Guarantee termination signals are sent to edge nodes even on crash
        for _ in range(config.SIMULATED_DEVICES_COUNT):
            try:
                asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()
            except Exception:
                pass


async def main(reader_instance, pipeline_mode: str, dataset_name: str):
    """
    The main asyncio engine bridging readers to the Spark cluster.
    """
    queue = asyncio.Queue(maxsize=config.SIMULATED_DEVICES_COUNT * 2)
    loop = asyncio.get_running_loop()

    # Initialize Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()

    # 1. Initialize a ThreadPoolExecutor to run the blocking yield_chunks() generators
    executor = ThreadPoolExecutor(max_workers=1)
    loop.run_in_executor(executor, blocking_reader_bridge, reader_instance, queue, loop)

    # Start benchmark timer
    start_time = time.perf_counter()

    # 2. Spin up hundereds of asynchronous virtual_edge_node coroutines
    tasks = []
    for i in range(config.SIMULATED_DEVICES_COUNT):
        task = asyncio.create_task(virtual_edge_node(i, queue, pipeline_mode, producer))
        tasks.append(task)

    # Wait for all nodes to finish processing the queue
    await asyncio.gather(*tasks)
    executor.shutdown(wait=True)

    await producer.stop()
    
    # End benchmark timer
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"BENCHMARK_RESULT|{dataset_name}|{pipeline_mode}|{benchmarks['total_chunks']}|{benchmarks['total_bytes']}|{elapsed_time:.2f}")

if __name__ == "__main__":
    import argparse
    from src.readers.chapman_reader import ChapmanReader
    from src.readers.mitbih_reader import MitBihReader
    from src.readers.peco_reader import PecoReader
    from src.readers.transfer_reader import TransferReader
    from src.readers.ptbxl_reader import PTBXLReader

    parser = argparse.ArgumentParser(description="ECG Stream Simulator Orchestrator")
    parser.add_argument('--dataset', type=str, required=True, 
                        choices=['chapman', 'mitbih', 'peco', 'transfer', 'ptbxl'],
                        help="Select the dataset reader to instantiate.")
    parser.add_argument('--pipeline', type=str, required=True,
                        choices=['A', 'B'],
                        help="Select Pipeline A (Raw Baseline) or Pipeline B (Edge-Assisted).")

    args = parser.parse_args()

    # Dictionary mapping CLI arguments to the specific reader classes
    reader_map = {
        'chapman': ChapmanReader,
        'mitbih': MitBihReader,
        'peco': PecoReader,
        'transfer': TransferReader,
        'ptbxl': PTBXLReader
    }

    # Dictionary mapping datasets to their zipfile locations
    path_map = {
        'chapman': 'data/chapman/raw/ECGData.zip',
        'mitbih': 'data/mitbih/raw/mit-bih-dataset.zip',
        'peco': 'data/ieee_peco/raw/peco-dataset.zip',
        'transfer': 'data/ieee_transfer_learning/raw/DataFall.zip',
        'ptbxl': 'data/ptbxl/raw/ptb-xl-dataset.zip'
    }
    # Instantiate the selected reader
    selected_reader_class = reader_map[args.dataset]
    dataset_path = path_map[args.dataset]

    reader_instance = selected_reader_class(dataset_path)

    print(f"Starting Stream Simulator | Dataset: {args.dataset.upper()} | Pipeline: {args.pipeline}")

    # Trigger the main asyncio event loop
    asyncio.run(main(reader_instance, pipeline_mode=args.pipeline, dataset_name=args.dataset))