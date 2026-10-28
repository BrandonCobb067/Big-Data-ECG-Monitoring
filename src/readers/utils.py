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

def clean_wfdb_header(hea_filepath: Path) -> List[str]:
    """
    Reads a .hea text file, strips surrounding whitespace, and removes comment lines.
    Returns a list of cleaned configuration lines.
    """
    with open(hea_filepath, "r", encoding="ascii") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return lines

def get_sorted_zip_entries(zip_source: zipfile.ZipFile) -> List[zipfile.ZipInfo]:
    """
    Returns a deterministically sorted list of files inside a ZIP archive to ensure consistent streaming and parallel partitioning.
    """
    return sorted(zip_source.infolist(), key = lambda entry: entry.filename)