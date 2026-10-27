from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord

class MitBihReader (BaseReader):
    LEADS = ["MLII"]

    def __init__ (self, raw_dir_path: str):
        self.raw_dir = Path(raw_dir_path)

    def _decode212(self, data: bytes, frame: int, channel: int) -> int:
        """Decodes Format 212 which packs two 12 bit values into three bytes"""
        offset = frame * 3
        middle_byte = data[offset + 1]

        if channel == 0:
            value = data[offset] | ((middle_byte & 15) << 8)
        else:
            value = data [offset + 2] | ((middle_byte & 240) << 4)

        if value >= 2048:
            value -= 4096

        return value

    def yield_chunks(self):
        """
        Stream 10-second (3600 sample) chunks from the MIT-BIH directory.
        """
        # Recursively find all .hea files in nested physionet.org structure
        for hea_file in sorted(self.raw_dir.rglob("*.hea")):
            record_id = hea_file.stem

            # Read the header file
            with hea_file.open ("r", encoding="ascii") as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
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
            dat_file = hea_file.with_suffix(".dat")
            if not dat_file.exists():
                continue

            packed_samples = dat_file.read_bytes()

            if len(packed_samples) != sample_count * 3:
                continue

            gain = float(fields[selected_channel][2])
            baseline = int(fields[selected_channel][4])
            samples = []

            # Extract the digital values and scale them to microvolts (uV)
            for sample in range (sample_count):
                digital_value = self._decode212(packed_samples, sample, selected_channel)
                if digital_value == -2048:
                    samples.append([float("nan")])
                else:
                    value = (digital_value - baseline) / gain * 1000.0
                    samples.append([value])

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
