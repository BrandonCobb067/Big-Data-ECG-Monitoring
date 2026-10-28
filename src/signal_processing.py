"""Filtering and waveform reconstruction shared by streaming and replay."""
import struct
import zlib

import numpy as np
from scipy.signal import firwin, lfilter

HEADER = struct.Struct("<4sIHHI")
FILTER_TAPS = 129


def make_filter(sample_rate):
    # Transition band: 0.20 to 0.25 times the original sampling rate.
    return firwin(FILTER_TAPS, 0.225 * sample_rate,
                  window=("kaiser", 8.0), fs=sample_rate)


def decode(packets, coefficients):
    windows = []
    pipeline = None
    for packet in packets:
        marker, start, rows, leads, sample_rate = HEADER.unpack(packet[:HEADER.size])
        payload = packet[HEADER.size:]
        if marker != b"RAW1":
            payload = zlib.decompress(payload)
        if len(payload) != rows * leads * 8:
            raise ValueError("Invalid packet size")
        windows.append(np.frombuffer(payload, dtype="<f8").reshape(rows, leads))
        pipeline = marker
    samples = np.concatenate(windows, axis=0)
    if pipeline == b"EDG1":
        # Restore the original sampling grid, then remove imaging frequencies.
        expanded = np.zeros((len(samples) * 2, samples.shape[1]))
        expanded[::2] = samples
        samples = lfilter(2 * coefficients, [1.0], expanded, axis=0)
    return samples


