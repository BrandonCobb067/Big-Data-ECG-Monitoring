import zipfile
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord
from src.readers.utils import get_sorted_zip_entries, extract_record_id, read_zip_wfdb_header, decode_212_signal

class MitBihReader (BaseReader):
    LEADS = ["MLII"]

    def __init__ (self, raw_zip_path: str):
        self.raw_zip_path = Path(raw_zip_path)

    def yield_chunks(self):
        """
        Stream 10-second (3600 sample) chunks from the MIT-BIH zip archive.
        """

        with zipfile.ZipFile(self.raw_zip_path, "r") as source:
            # Sort to ensure consistent processing order
            entries = get_sorted_zip_entries(source)

            # Iterate through the zip archive looking for header files 
            for entry in entries:
                if entry.is_dir() or not entry.filename.endswith(".hea"):
                    continue

                # Extract ID and strip the .hea extension and path
                record_id = extract_record_id(entry.filename, ".hea")

                # Read the header file directly from the zip in memory
                # Read the header file directly from the zip in memory
                with source.open(entry) as f:
                    lines = read_zip_wfdb_header(f)

                if not lines:
                    continue

                header = lines[0].split()
                sample_count = int(header[3])

                # Verify the 2-channel 360Hz format 
                if header[1] != "2" or header [2] != "360":
                    continue

                fields = [lines[1].split(), lines[2].split()]
                selected_channel = -1

                # Locate the MLII lead
                for channel in range(2):
                    lead_name = fields[channel][-1]
                    if lead_name == "MLII":
                        selected_channel = channel

                if selected_channel < 0:
                    continue # Skip records without the MLII lead

                # Find and read the corresponding binary .dat file
                dat_filename = entry.filename.replace(".hea", ".dat")
                try:
                    dat_entry = source.getinfo(dat_filename)
                except KeyError:
                    continue

                with source.open(dat_entry) as dat_file:
                    packed_samples = dat_file.read()

                if len(packed_samples) != sample_count * 3:
                    continue

                gain = float(fields[selected_channel][2])
                baseline = int(fields[selected_channel][4])
                samples = []

                # Extract the digital values and scale them to microvolts (uV)
                samples = decode_212_signal(packed_samples, sample_count, selected_channel, baseline, gain)

                # Slice into 10-second chunks (3600 samples at 360 Hz)
                for start in range(0, sample_count, 3600):
                    length = min(3600, sample_count - start)

                    # Yield standard full chunks (skip short tail fragments)
                    if length == 3600:
                        yield ECGRecord(
                            record_id=f"{record_id}_{start:07d}",
                            sample_rate=360,
                            leads=self.LEADS.copy(),
                            units="uV",
                            samples=samples[start:start + length]
                        )
