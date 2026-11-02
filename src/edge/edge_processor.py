"""
Simulates the local mathematical processing of an IoT edge device for Pipeline B
"""

import asyncio
import numpy as np
from scipy.signal import butter, filtfilt
import src.config as config

def apply_butterworth_filter(data: np.ndarray, lowcut: float, highcut: float, fs: int, order: int = 4) -> np.ndarray:
    """
    Applies a lightweight Butterworth bandpass filter to remove baseline wander and high frequency noise prior to downsampling to prevent aliasing.
    """
    nyquist = 0.5 * fs
    low = lowcut / nyquist
    high = highcut / nyquist

    # Ensure the high cutoff does not exceed the Nyquist frequency
    high = min(high, 0.99)

    b, a = butter(order, [low, high], btype = 'band')

    # Apply filter along the time axis (assuming shape is [samples, leads])
    return filtfilt(b, a, data, axis=0)

async def process_edge_chunk(ecg_samples: np.ndarray, original_fs: int, target_fs: int, lowcut: float = 0.5) -> np.ndarray:
    """
    Executes the Pipeline B edge compute simulation on a 10-second chunk of ECG data
    """
    # 1. Simulate Edge Compute Delay
    await asyncio.sleep(config.COMPUTE_DELAY_SECONDS)

    # 2. Anti-Aliasing (Bandpass Filter)
    # The high cutoff is set just below the Nyquist limit of the target frequency (target_fs / 2)
    highcut = (target_fs / 2.0) - 1.0
    filtered_data = apply_butterworth_filter(ecg_samples, lowcut, highcut, original_fs)

    # 3. Decimation (Subsampling)
    # Calculate integer factor N and slice the array to reduce the frequency
    decimation_factor = int(original_fs // target_fs)
    downsampled_data = filtered_data[::decimation_factor]

    # 4 Quantization (Precision Reduction)
    # Convert from 64-bit float down to 16-bit float to compress the payload
    quantized_payload = downsampled_data.astype(np.float16)

    return quantized_payload