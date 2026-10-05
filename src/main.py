import csv
import hashlib
import io
import math
import re
import struct
import sys
import time
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


LEADS = [
    "I", "II", "III", "aVR", "aVL", "aVF",
    "V1", "V2", "V3", "V4", "V5", "V6"
]


class EcgRecord:
    def __init__(self, record_id, sample_rate, leads, units, samples):
        self.record_id = record_id
        self.sample_rate = sample_rate
        self.leads = leads
        self.units = units
        self.samples = samples


class Cleaning:
    def __init__(self, filled_values, flat_leads):
        self.filled_values = filled_values
        self.flat_leads = flat_leads


def read_chapman(record_id, input_file):
    header = input_file.readline().decode("utf-8")
    if not header:
        raise OSError("Unexpected lead header")

    header_leads = header.replace("\ufeff", "").strip().split(",")
    if header_leads != LEADS:
        raise OSError("Unexpected lead header")

    samples = []

    for raw_line in input_file:
        line = raw_line.decode("utf-8")
        if not line.strip():
            continue

        fields = line.rstrip("\r\n").split(",")
        if len(fields) != 12:
            raise OSError("Expected 12 columns")

        row = []
        for lead in range(12):
            value = fields[lead].strip()

            try:
                if not value:
                    row.append(float("nan"))
                else:
                    row.append(float(value))
            except ValueError as exception:
                raise OSError("Non-numeric value: " + value) from exception

        samples.append(row)

    if len(samples) != 5000:
        raise OSError("Expected 5000 samples, found " + str(len(samples)))

    return EcgRecord(record_id, 500, LEADS.copy(), "uV", samples)


def clean(record):
    samples = record.samples
    filled_values = 0
    flat_leads = 0

    for lead in range(len(record.leads)):
        missing_values = 0

        for row in samples:
            if not math.isfinite(row[lead]):
                missing_values += 1

        if missing_values > len(samples) * 0.01:
            raise OSError("More than 1% missing in lead " + record.leads[lead])

        filled_values += missing_values

        # Fill missing samples using the surrounding values.
        for sample in range(len(samples)):
            if math.isfinite(samples[sample][lead]):
                continue

            left = sample - 1
            right = sample + 1

            while right < len(samples) and not math.isfinite(samples[right][lead]):
                right += 1

            if left < 0 and right == len(samples):
                raise OSError("Empty lead")

            if left < 0:
                samples[sample][lead] = samples[right][lead]
            elif right == len(samples):
                samples[sample][lead] = samples[left][lead]
            else:
                difference = samples[right][lead] - samples[left][lead]
                samples[sample][lead] = (
                    samples[left][lead]
                    + difference * (sample - left) / (right - left)
                )

        minimum = float("inf")
        maximum = float("-inf")

        for row in samples:
            minimum = min(minimum, row[lead])
            maximum = max(maximum, row[lead])

        # Count flat leads, but keep them in the output.
        if maximum - minimum < 0.000001:
            flat_leads += 1

    return Cleaning(filled_values, flat_leads)


def signal_hash(record):
    digest = hashlib.sha256()
    buffer = bytearray()

    for row in record.samples:
        for value in row:
            # Treat positive and negative zero as the same waveform value.
            if value == 0:
                value = 0.0

            buffer.extend(struct.pack(">d", value))

    digest.update(buffer)
    return digest.hexdigest()


def write_csv(output, filename, record, start, length):
    csv_text = io.StringIO()
    writer = csv.writer(csv_text, lineterminator="\n")
    writer.writerow(record.leads)

    for row in range(start, start + length):
        writer.writerow(record.samples[row])

    output.writestr(filename, csv_text.getvalue().encode("utf-8"))


def clean_chapman():
    folder = Path("data/chapman")
    raw_file = folder / "raw/ECGData.zip"
    cleaned_file = folder / "cleaned/ECGData-cleaned.zip"

    if cleaned_file.exists():
        raise OSError("Cleaned output already exists. Rename it before another run.")

    cleaned_file.parent.mkdir(parents=True, exist_ok=True)
    partial_file = cleaned_file.with_name("ECGData-cleaned.zip.part")
    log_file = folder / "cleaning-log.tsv"

    hashes = set()
    record_ids = set()
    scanned = 0
    accepted = 0
    rejected = 0
    filled_values = 0
    flat_leads = 0
    started = time.perf_counter()

    # Read one record at a time and keep the raw zip unchanged.
    with (
        ZipFile(raw_file, "r") as source,
        ZipFile(partial_file, "w", compression=ZIP_DEFLATED, compresslevel=1) as output,
        log_file.open("w", encoding="utf-8", newline="") as log
    ):
        log.write("record\tstatus\tfilled_values\tflat_leads\treason\n")
        entries = sorted(source.infolist(), key=lambda entry: entry.filename)

        for entry in entries:
            if entry.is_dir() or not entry.filename.endswith(".csv"):
                continue

            scanned += 1
            filename = entry.filename.rsplit("/", 1)[-1]
            record_id = filename[:-4]

            try:
                with source.open(entry) as input_file:
                    record = read_chapman(record_id, input_file)
                    result = clean(record)

                    if record_id in record_ids:
                        raise OSError("Duplicate record ID")
                    record_ids.add(record_id)

                    waveform_hash = signal_hash(record)
                    if waveform_hash in hashes:
                        raise OSError("Duplicate waveform")
                    hashes.add(waveform_hash)
            except OSError as exception:
                rejected += 1
                reason = str(exception).replace("\t", " ")
                log.write(f"{record_id}\trejected\t0\t0\t{reason}\n")
                continue

            write_csv(output, filename, record, 0, len(record.samples))
            accepted += 1
            filled_values += result.filled_values
            flat_leads += result.flat_leads
            log.write(
                f"{record_id}\taccepted\t{result.filled_values}"
                f"\t{result.flat_leads}\t\n"
            )

            if scanned % 1000 == 0:
                print(f"Processed {scanned} Chapman records")

    if accepted == 0:
        raise OSError("No usable records; inspect cleaning-log.tsv")

    partial_file.rename(cleaned_file)
    elapsed_seconds = int(time.perf_counter() - started)
    summary = (
        "Dataset: original Chapman ECG\n"
        f"Raw records checked: {scanned}\n"
        f"Accepted: {accepted}\n"
        f"Rejected: {rejected}\n"
        f"Values filled: {filled_values}\n"
        f"Flat leads flagged: {flat_leads}\n"
        "Sampling rate: 500 Hz\n"
        "Leads: 12\n"
        "Samples per record: 5000\n"
        "Units retained: microvolts (uV)\n"
        f"Elapsed seconds: {elapsed_seconds}\n"
    )

    (folder / "summary.txt").write_text(summary, encoding="utf-8")
    print(summary)


# Format 212 stores two signed 12-bit values in three bytes.
def decode212(data, frame, channel):
    offset = frame * 3
    middle_byte = data[offset + 1]

    if channel == 0:
        value = data[offset] | ((middle_byte & 15) << 8)
    else:
        value = data[offset + 2] | ((middle_byte & 240) << 4)

    if value >= 2048:
        value -= 4096

    return value


def clean_mitbih():
    folder = Path("data/mitbih")
    raw_file = folder / "raw/mitbih.zip"
    cleaned_file = folder / "cleaned/MLII-cleaned.zip"

    if cleaned_file.exists():
        raise OSError("Cleaned output already exists. Rename it before another run.")

    cleaned_file.parent.mkdir(parents=True, exist_ok=True)
    partial_file = cleaned_file.with_name("MLII-cleaned.zip.part")
    log_file = folder / "cleaning-log.tsv"

    hashes = set()
    checked = 0
    accepted = 0
    skipped = 0
    full_fragments = 0
    tail_fragments = 0
    filled_values = 0
    flat_leads = 0
    started = time.perf_counter()

    with (
        ZipFile(raw_file, "r") as source,
        ZipFile(partial_file, "w", compression=ZIP_DEFLATED) as output,
        log_file.open("w", encoding="utf-8", newline="") as log
    ):
        log.write(
            "record\tstatus\tfilled_values\tflat_leads\tfull_fragments\ttail_samples\treason\n"
        )
        entries = sorted(source.infolist(), key=lambda entry: entry.filename)

        for entry in entries:
            entry_name = entry.filename
            filename = entry_name.rsplit("/", 1)[-1]

            if not re.fullmatch(r"[0-9]{3}\.hea", filename) or "/mitdbdir/" in entry_name:
                continue

            checked += 1
            record_id = filename[:3]
            prefix = entry_name[:entry_name.rfind("/") + 1]
            lines = []

            with source.open(entry) as input_file:
                for raw_line in input_file:
                    line = raw_line.decode("ascii").rstrip("\r\n")
                    if line.strip() and not line.startswith("#"):
                        lines.append(line)

            header = lines[0].split()
            sample_count = int(header[3])
            selected_channel = -1

            if header[1] != "2" or header[2] != "360":
                raise OSError("Unexpected MIT-BIH header " + record_id)

            fields = [lines[1].split(), lines[2].split()]

            for channel in range(2):
                if fields[channel][1] != "212" or fields[channel][0] != record_id + ".dat":
                    raise OSError("Unsupported encoding " + record_id)

                lead_name = fields[channel][-1]
                if lead_name == "MLII":
                    selected_channel = channel

            if selected_channel < 0:
                skipped += 1
                log.write(f"{record_id}\tskipped\t0\t0\t0\t0\tNo MLII lead\n")
                continue

            packed_samples = source.read(prefix + record_id + ".dat")
            if len(packed_samples) != sample_count * 3:
                raise OSError("Unexpected data length " + record_id)

            # This release uses numeric gains, mV units, and ADC zero as baseline.
            gain = float(fields[selected_channel][2])
            baseline = int(fields[selected_channel][4])

            if not gain > 0:
                raise OSError("Invalid gain")

            samples = []
            checksums = [0, 0]

            for sample in range(sample_count):
                for channel in range(2):
                    checksums[channel] += decode212(packed_samples, sample, channel)

                digital_value = decode212(packed_samples, sample, selected_channel)
                if digital_value == -2048:
                    samples.append([float("nan")])
                else:
                    value = (digital_value - baseline) / gain * 1000.0
                    samples.append([value])

            for channel in range(2):
                expected_checksum = int(fields[channel][6])
                # Only the lowest 16 bits are used for the checksum.
                if (checksums[channel] & 65535) != (expected_checksum & 65535):
                    raise OSError("WFDB checksum mismatch " + record_id)

                expected_first_sample = int(fields[channel][5])
                if decode212(packed_samples, 0, channel) != expected_first_sample:
                    raise OSError("Initial sample mismatch " + record_id)

            record = EcgRecord(record_id, 360, ["MLII"], "uV", samples)

            try:
                result = clean(record)
                waveform_hash = signal_hash(record)

                if waveform_hash in hashes:
                    raise OSError("Duplicate waveform")
                hashes.add(waveform_hash)
            except OSError as exception:
                skipped += 1
                log.write(f"{record_id}\trejected\t0\t0\t0\t0\t{exception}\n")
                continue

            # At 360 Hz, 3600 samples make a ten-second fragment.
            for start in range(0, sample_count, 3600):
                length = min(3600, sample_count - start)
                fragment_name = f"{record_id}_{start:07d}"

                if length < 3600:
                    fragment_name += "_tail"
                    tail_fragments += 1
                else:
                    full_fragments += 1

                fragment_name += ".csv"
                write_csv(output, fragment_name, record, start, length)

            accepted += 1
            filled_values += result.filled_values
            flat_leads += result.flat_leads
            log.write(
                f"{record_id}\taccepted\t{result.filled_values}\t{result.flat_leads}"
                f"\t{sample_count // 3600}\t{sample_count % 3600}\t\n"
            )

    if checked != 48 or accepted == 0:
        raise OSError("Unexpected record count; inspect input archive")

    partial_file.rename(cleaned_file)
    elapsed_seconds = int(time.perf_counter() - started)
    summary = (
        "Dataset: MIT-BIH Arrhythmia Database 1.0.0\n"
        f"Raw records: {checked}\n"
        f"Accepted MLII recordings: {accepted}\n"
        f"Skipped/rejected: {skipped}\n"
        f"Full ten-second fragments: {full_fragments}\n"
        f"Short tail fragments retained: {tail_fragments}\n"
        f"Values filled: {filled_values}\n"
        f"Flat leads flagged: {flat_leads}\n"
        "Sampling rate: 360 Hz\n"
        "Units: microvolts (uV)\n"
        "WFDB checksums and initial samples verified for accepted recordings\n"
        f"Elapsed seconds: {elapsed_seconds}\n"
    )

    (folder / "summary.txt").write_text(summary, encoding="utf-8")
    print(summary)


def test():
    samples = []
    for sample in range(100):
        samples.append([float(sample)])

    samples[20][0] = float("nan")
    record = EcgRecord("test", 360, ["MLII"], "uV", samples)

    if clean(record).filled_values != 1 or samples[20][0] != 20:
        raise AssertionError("Interpolation failed")

    samples[1][0] = float("nan")
    samples[2][0] = float("nan")
    rejected = False

    try:
        clean(record)
    except OSError:
        rejected = True

    if not rejected:
        raise AssertionError("Missing-data threshold failed")

    negative_samples = [[-5.0], [-3.0], [-1.0]]
    clean(EcgRecord("negative", 500, ["II"], "uV", negative_samples))

    if negative_samples[0][0] != -5:
        raise AssertionError("Negative values changed")

    signed_samples = bytes([255, 143, 0])
    if decode212(signed_samples, 0, 0) != -1 or decode212(signed_samples, 0, 1) != -2048:
        raise AssertionError("Signed WFDB decoding failed")

    positive_samples = bytes([227, 3, 1])
    if decode212(positive_samples, 0, 0) != 995 or decode212(positive_samples, 0, 1) != 1:
        raise AssertionError("Channel decoding failed")

    print(
        "5 checks passed: interpolation, missing threshold, negative values, "
        "signed WFDB decoding, channel decoding."
    )


def main():
    if len(sys.argv) != 2:
        print("Use: python main.py clean-chapman | clean-mitbih | test")
        return

    command = sys.argv[1]

    if command == "clean-chapman":
        clean_chapman()
    elif command == "clean-mitbih":
        clean_mitbih()
    elif command == "test":
        test()
    else:
        raise ValueError("Unknown command")


if __name__ == "__main__":
    main()
