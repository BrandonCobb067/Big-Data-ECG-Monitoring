import math
from src.models.ecg_record import ECGRecord

class CleaningResult:
    def __init__(self, filled_values, flat_leads):
        self.filled_values = filled_values
        self.flat_leads = flat_leads

class DataCleaner:
    @staticmethod
    def clean(record:ECGRecord) -> CleaningResult:
        samples = record.samples
        filled_values = 0
        flat_leads = 0

        # Iterate over the number of leads in the standardized object
        for lead in range(len(record.leads)):
            missing_values = 0

            for row in samples: 
                if not math.isfinite(row[lead]):
                    missing_values += 1

            # Reject the chunk if missing data exceeds 1%
            if missing_values > len(samples) * 0.01:
                raise ValueError("more than 1% missing in lead " + record.leads[lead])

            filled_values += missing_values

            # Fill missing samples using the surrounding values
            for sample in range(len(samples)):
                if math.isfinite(samples[sample][lead]):
                    continue

                left = sample - 1
                right = sample + 1

                while right < len(samples) and not math.isfinite(samples[right][lead]):
                    right += 1

                if left < 0 and right == len(samples):
                    raise ValueError("Empty lead")

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

            # Count flat leads (sensor fell off) but keep them in the output
            if maximum - minimum < 0.000001:
                flat_leads += 1

        return CleaningResult(filled_values, flat_leads)