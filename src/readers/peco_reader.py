import re
import zipfile
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.utils import get_sorted_zip_entries, extract_record_id, read_zip_wfdb_header, decode212, digital_to_microvolts

class PecoReader(BaseReader):
    def __init__(self, raw_zip_path: str):
        # Initialization for path to compressed zip
        self.raw_zip_path = Path(raw_zip_path)

    def yield_chunks(self):
        """Streams 10 second chunks from the PECO WFDB files from the zip archive"""
        with zipfile.ZipFile(self.raw_zip_path, "r") as source:
            # Map all files in the zip for easy cross-referencing between .hea and .dat
            sorted_list = get_sorted_zip_entries(source)
            entries = {entry.filename: entry for entry in sorted_list}

            for filename, entry in entries.items():
                # Target the ECG_DAT subdrectory, ignoring PCG data
                if not filename.endswith(".hea") or "ECG_DAT/" not in filename:
                    continue

                record_id = extract_record_id(filename, ".hea")
                dat_filename = filename.replace(".hea", ".dat")

                if dat_filename not in entries:
                    continue

                # Read and clean header lines directly from zip byte stream
                with source.open(entry) as hf:
                    lines = read_zip_wfdb_header(hf)

                if not lines:
                    continue
   
                # Parse the primary header line
                header = lines[0].split()
                
                # Ensure header has enough columns (Name, Signals, SampleRate, SampleCount)
                if len(header) < 4:
                    continue
                    
                sample_rate = int(header[2])
                sample_count = int(header[3])
                chunk_size = sample_rate * 10 

                if len(lines) < 2:
                    continue

                # Parse the channel specification line (assuming single-lead ECG for PECO)
                channel_info = lines[1].split()

                if len(channel_info) < 3:
                    continue

                raw_gain = channel_info[2].strip()
                gain = float(re.split(r'[\(/]', raw_gain)[0]) 

                if len(channel_info) > 4:
                    baseline = int(channel_info[4])
                else:
                    baseline = 0

                if len(channel_info) > 8:
                    lead_name = channel_info[-1]
                else:
                    lead_name = "Unknown"

                if gain <= 0:
                    continue

                # Read the binary .dat file
                with source.open(entries[dat_filename]) as df:
                    packed_samples = df.read()

                if len(packed_samples) < sample_count * 1.5: 
                    continue

                # Decode the 1-channel Format 212 signal
                # Two consecutive samples of the same channel are packed in one 3-byte frame
                full_samples = []
                for sample_idx in range(sample_count):
                    frame = sample_idx // 2
                    sub_channel = sample_idx % 2
                    
                    # Protect against out of range errors from malformed sample counts
                    if frame * 3 + 2 >= len(packed_samples):
                        break
                        
                    digital_value = decode212(packed_samples, frame, sub_channel)
                    value = digital_to_microvolts(digital_value, baseline, gain)
                    full_samples.append([value])

                actual_sample_count = len(full_samples)

                # Yield in 10-second chunks
                for start in range(0, actual_sample_count, chunk_size):
                    length = min(chunk_size, actual_sample_count - start)

                    # Only yield full 10-second chunks to maintain standardizations
                    if length == chunk_size:
                        chunk_id = f"{record_id}_{start:07d}"
                        yield ECGRecord(
                            record_id=chunk_id,
                            sample_rate=sample_rate,
                            leads=[lead_name],
                            units="uV",
                            samples=full_samples[start: start + length]
                        )