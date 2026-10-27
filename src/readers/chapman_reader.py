import zipfile
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord

class ChapmanReader(BaseReader):
    LEADS = [
        "I", "II", "III", "aVR", "aVL", "aVF",
        "V1", "V2", "V3", "V4", "V5", "V6"
    ]

    def __init__ (self, raw_zip_path: str):
        self.raw_zip_path = Path(raw_zip_path)

    def yield_chunks(self):
        """
        Streams 10-second (5000 sample) chunks from the Chapman zip archive
        """
        with zipfile.ZipFile(self.raw_zip_path, "r") as source:
            # Sort to ensure consistent processing order
            entries = sorted(source.infolist(), key=lambda entry: entry.filename)

            for entry in entries:
                if entry.is_dir() or not entry.filename.endswith(".csv"):
                    continue

                filename = entry.filename.rsplit("/", 1)[-1]
                record_id = filename [:-4] # strip the .csv extension

                with source.open(entry) as input_file:
                    # Read and validate the header
                    header = input_file.readline().decode("utf-8")
                    if not header:
                        continue

                    header_leads = header.replace("\ufeff", "").strip().split(",")
                    if header_leads != self.LEADS:
                        raise OSError(f"Unexpected lead header in {record_id}")

                    samples = []
                    for raw_line in input_file:
                        line = raw_line.decode("utf-8").strip()
                        if not line:
                            continue

                        fields = line.rstrip("\r\n").split(",")
                        if len(fields) != 12:
                            raise OSError(f"Expected 12 columns in {record_id}")

                        row = []
                        for lead in range(12):
                            value = fields[lead].strip()
                            try:
                                # Handle missing values immediately by inserting NaN
                                if not value:
                                    row.append(float("nan"))
                                else:
                                    row.append(float(value))
                            except ValueError as exception:
                                raise OSError(f"Non-numeric value: {value}") from exception

                        samples.append(row)

                    if len(samples) == 5000:
                        yield ECGRecord(
                            record_id=record_id,
                            sample_rate=500,
                            leads=self.LEADS.copy(),
                            units="uV",
                            samples=samples
                        )