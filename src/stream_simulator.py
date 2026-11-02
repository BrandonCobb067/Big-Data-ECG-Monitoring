"""
Asynchronous orchestrator to simulate concurrent vital edge nodes transmitting ECG data
"""

import asyncio
import json
import socket
from concurrent.futures import ThreadPoolExecutor
import numpy as np

import src.config as config
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.edge.edge_processor import process_edge_chunk

class NumpyEncoder(json.JSONEncoder):
    """Custom encoder to serialize NumPy arrays to JSON."""
    def default (self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.float16):
            return float(obj)
        return super().default(obj)

async def virtual_edge_node(node_id: int, queue: asyncio.Queue, pipeline_mode: str):
    """
    Simulates a single concurrent IoT device processing and transmitting data
    """
    # Establish a non-blocking connection to Spark's TCP Receiver
    reader, writer = await asyncio.open_connection(
        config.SPARK_RECEIVER_IP,
        config.SPARK_RECEIVER_PORT  
    )

    try:
        while True:
            # Pull the next 10-second chunk from the thread-pool bridge
            ecg_record = await queue.get()

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
            serialized_data = json.dumps(payload,cls=NumpyEncoder) + "\n"
            writer.write(serialized_data.encode('utf8'))
            await writer.drain()

            queue.task_done()

    except Exception as e:
        print(f"Node {node_id} disconnected: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
