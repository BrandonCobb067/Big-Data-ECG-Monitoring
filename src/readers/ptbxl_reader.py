import struct
import zipfile
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.readers.utils import get_sorted_zip_entries

class PTBXLReader(BaseReader):
    LEADS = [
            "I", "II", "III", "AVL", "AVR", "AVF",
            "V1", "V2", "V3", "V4", "V5", "V6"
        ]

    def __init__(self, raw_zip_path: str):
        # Initialization accepting the specific raw dataset file path
        self.raw_zip_path = Path(raw_zip_path)

    def yield_chunks(self):
        """
        Streams 10-second (5000 sample) chunks from teh PTB-XL zip archive. 
        Targets the records500/ directory for 500 Hz sampling
        """
        sample_rate = 500
        expected_samples = 5000 # 10 seconds * 500 Hz
        target_folder = "records500/"

        with zipfile.ZipFile(self.raw_zip_path, "r") as source:
            # Sort to ensure consistent processing order
            entries = get_sorted_zip_entries(source)

            for entry in entries:
                if entry.is_dir() or target_folder not in entry.filename or not entry.filename.endswith(".dat"):
                    continue

                # Extract ID and strip the _hr.dat extension
                filename = entry.filename.rsplit("/", 1)[-1]
                record_id = filename.replace("_hr.dat", "")

                with source.open(entry) as dat_file:
                    raw_bytes = dat_file.read()

                    # 12 leads * 2 bytes per sample (16-bit precision)
                    expected_bytes = expected_samples * 12 * 2
                    if len(raw_bytes) != expected_bytes:
                        continue

                    # Unpack as little-endian 16-bit signed integers ('h')
                    unpacked_data = struct.unpack(f"<{expected_samples * 12}h", raw_bytes)

                    samples = []
                    # Group the flattened 1D array into rows of 12 leads
                    for i in range (expected_samples):
                        row = list(unpacked_data[i * 12 : (i + 1) * 12])
                        # PTB-XL resolution is 1UV/LSB, so standard float casting maps directly to microvolts
                        samples.append([float(val) for val in row])

                    yield ECGRecord(
                        record_id=record_id,
                        sample_rate=sample_rate,
                        leads=self.LEADS.copy(),
                        units="uV",
                        samples=samples
                    )