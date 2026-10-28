from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.readers.utils import decode212, digital_to_microvolts, clean_wfdb_header

class PecoReader(BaseReader):
    def __init__(self, raw_dir_path: str):
        # Specifically target the ECG_Dat subdirectory, ignoring PCG data
        self.ecg_dir = Path(raw_dir_path) / "ECG_DAT"

    def yield_chunks(self):
        """Streams 10-second chunks from teh PECO WFDB files"""
        if not self.ecg_dir.exists():
            raise FileNotFoundError(f"Directory not found: {self.ecg_dir}")

        # iterate through all header files in the ECG_DAT folder
        for hea_file in sorted(self.ecg_dir.glob("*.hea")):
            record_id = hea_file.stem
            dat_file = self.ecg_dir / f"{record_id}.dat"

            if not dat_file.exists():
                continue

            lines = clean_wfdb_header(hea_file)
            if not lines:
                continue

            # Parse the primary header line
            header = lines[0].split()
            sample_rate = int(header[2])
            sample_count = int(header[3])
            chunk_size = sample_rate * 10 # 10 seconds of data

            # Parse the channel specification line (assuming single-lead ECG for PECO)
            channel_info = lines[1].split()
            gain = float(channel_info[2])
            baseline = int(channel_info[4])
            lead_name = channel_info[-1]

            if gain <= 0:
                continue

            # Read the binary .dat file
            with open(dat_file, "rb") as df:
                packed_samples = df.read()

            if len(packed_samples) < sample_count * 1.5: #1.5 bytes per sample in 212
                continue

            # Decode the entire record and convert to microvolts
            full_samples = []
            for sample_idx in range(sample_count):
                digital_value = decode212(packed_samples, sample_idx, 0)
                value = digital_to_microvolts(digital_value, baseline, gain)
                full_samples.append([value])

            # Yield in 10-second chunks
            for start in range(0, sample_count, chunk_size):
                length = min(chunk_size, sample_count - start)

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