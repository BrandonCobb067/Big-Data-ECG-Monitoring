"""Shared framing and stateful edge preprocessing."""
import socket
import struct
import zlib

import numpy as np
from bson import BSON
from scipy.signal import lfilter
from src.signal_processing import HEADER, make_filter

MAX_MESSAGE = 4 * 1024 * 1024


def receive_exact(connection, size):
    data = bytearray()
    while len(data) < size:
        block = connection.recv(size - len(data))
        if not block:
            raise EOFError("Connection closed before complete frame")
        data.extend(block)
    return bytes(data)


def send_message(connection, document):
    body = BSON.encode(document)
    if len(body) > MAX_MESSAGE:
        raise ValueError("Message exceeds limit")
    connection.sendall(struct.pack("!I", len(body)) + body)
    return len(body) + 4


def receive_message(connection):
    size = struct.unpack("!I", receive_exact(connection, 4))[0]
    if size > MAX_MESSAGE or size < 5:
        raise ValueError("Invalid frame size")
    return BSON(receive_exact(connection, size)).decode(), size + 4


class EdgeEncoder:
    def __init__(self, sample_rate, leads, mode):
        self.sample_rate = sample_rate
        self.mode = mode
        self.coefficients = make_filter(sample_rate)
        self.state = np.zeros((len(self.coefficients) - 1, leads))

    def encode(self, samples, start):
        output = samples
        rate = self.sample_rate
        aggregates = {}
        marker = b"RAW1"
        if self.mode == "edge":
            filtered, self.state = lfilter(self.coefficients, [1.0], samples, axis=0, zi=self.state)
            output = filtered[start % 2::2]
            rate //= 2
            marker = b"EDG1"
            aggregates = {"mean_uv": output.mean(axis=0).tolist(),
                          "std_uv": output.std(axis=0).tolist()}
        body = np.asarray(output, dtype="<f8").tobytes()
        if self.mode == "edge":
            body = zlib.compress(body, 6)
        return HEADER.pack(marker, start, len(output), output.shape[1], rate) + body, aggregates


def unpack_tensor(packet):
    marker, start, rows, leads, rate = HEADER.unpack(packet[:HEADER.size])
    if marker not in (b"RAW1", b"EDG1") or rows < 1 or leads not in (1, 12):
        raise ValueError("Invalid ECG header")
    if rate not in (180, 250, 360, 500) or rows > 1000:
        raise ValueError("Unexpected rate or window size")
    body = packet[HEADER.size:]
    if marker == b"EDG1":
        decoder = zlib.decompressobj()
        body = decoder.decompress(body, rows * leads * 8 + 1)
        if not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
            raise ValueError("Invalid compressed tensor")
    if len(body) != rows * leads * 8:
        raise ValueError("Incorrect tensor length")
    samples = np.frombuffer(body, dtype="<f8").reshape(rows, leads)
    if not np.isfinite(samples).all():
        raise ValueError("Nonfinite samples")
    return samples, rate
