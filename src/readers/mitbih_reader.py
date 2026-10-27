import zipfile
import re
from pathlib import Path
from src.readers.base_reader import BaseReader
from src.models.ecg_record import ECGRecord

class MitBihReader (BaseReader):
    Leads = ["MLII"]

    def __init__ (self, raw_zip_path: str):
        self.raw_zip_path = Path(raw_zip_path)

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
        ##TO DO 