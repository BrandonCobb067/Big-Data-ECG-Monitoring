import zipfile
import re
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.readers.utils import extract_record_id, get_sorted_zip_entries

class TransferReader(BaseReader):
    def __init__(self, raw_zip_path: str, sample_rate: int = 250):
        # Sample rate is configurable; defaults to a standard 250HZ for wearables
        self.raw_zip_path = Path(raw_zip_path)
        self.sample_rate = sample_rate
        self.chunk_size = sample_rate * 10 # 10 second segments

    def yield_chunks(self):
        """
        Streams 10-second chunks from the IEEE Transfer Learning zip archive ignoring noisy 'X' folders and extracting data from '3-TXT files'
        """
        with zipfile.ZipFile(self.raw_zip_path, "r") as source:
            entries = get_sorted_zip_entries(source)
            
            for entry in entries:
                filename = entry.filename

                # Ignore directories, non-txt files, and noisy 'X' subject folders
                if entry.is_dir() or not filename.endswith(".txt"):
                    continue
                if re.search(r'sub\d+X/', filename, re.IGNORECASE):
                    continue
                if "3-TXT Files" not in filename:
                    continue

                # Extract the base filename (e.g., 's2r3') to serve as the record_id
                base_name = extract_record_id(filename, ".txt")

                with source.open(entry) as input_file:
                    samples = []
                    for raw_line in input_file:
                        line = raw_line.decode("utf-8").strip()
                        if not line:
                            continue

                        try:
                            # Transfer text files contain single-column numeric strings
                            value = float(line)
                            samples.append([value])
                        except ValueError as exception:
                            # Handle non-numeric artifacts by inserting NaN
                            samples.append([float("nan")])

                        # Yield exactly 10 seconds of data at a time
                        if len(samples) == self.chunk_size:
                            yield ECGRecord(
                                record_id=base_name,
                                sample_rate=self.sample_rate,
                                leads=["Single-Lead"],
                                units="mV",
                                samples=samples
                            )
                            samples = []
