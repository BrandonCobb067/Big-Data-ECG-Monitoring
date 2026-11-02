import zipfile
from pathlib import Path
from typing import List

def decode212(data: bytes, frame: int, channel: int) -> int:
    """
    Decodes Format 212, which packs two signed 12-bit values into three bytes.
    Used by WFDB formats like MIT-BIH and PECO
    """
    offset = frame * 3
    middle_byte = data[offset + 1]

    if channel == 0:
        value = data[offset] | ((middle_byte & 15) << 8)
    else:
        value = data[offset + 2] | ((middle_byte & 240) << 4)

    if value >= 2048:
        value -= 4096

    return value

def digital_to_microvolts(digital_value: int, baseline: int, gain: float) -> float:
    """
    Converts a raw digital integer from a WFDB file into a physical microvolt (uV) measurement.
    Handles the -2048 NaN identifier.
    """
    if digital_value == -2048:
        return float("nan")

    return (digital_value - baseline) / gain * 1000.0

def read_zip_wfdb_header(hf) -> List [str]:
    """
    Reads a WFDB .hea file directly from a zipfile byte stream.
    Decodes from ASCII, strips whitespace, and removes comment lines.
    """
    return [
        line.decode ("ascii").strip()
        for line in hf
        if line.strip() and not line.decode("ascii").startswith("#")
    ]

def get_sorted_zip_entries(zip_source: zipfile.ZipFile) -> List[zipfile.ZipInfo]:
    """
    Returns a deterministically sorted list of files inside a ZIP archive to ensure consistent streaming and parallel partitioning.
    """
    return sorted(zip_source.infolist(), key = lambda entry: entry.filename)

def extract_record_id(filepath: str, extension: str) -> str:
    """
    Extracts the base record ID from a nested zip filepath by removing directories and the file extension.
    """
    return filepath.rsplit("/", 1)[-1].replace(extension, "")

def decode_212_signal(packed_samples: bytes, sample_count: int, channel: int, baseline: int, gain: float) -> List[List[float]]:
    """
    Decodes an entire Format 212 byte stream into a list of microvolt samples.
    """
    samples = []
    for sample_idx in range(sample_count):
        digital_value = decode212(packed_samples, sample_idx, channel)
        value = digital_to_microvolts(digital_value, baseline, gain)
        samples.append([value])
    return samples