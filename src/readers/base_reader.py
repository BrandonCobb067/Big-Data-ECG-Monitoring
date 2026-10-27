from abc import ABC, abstractmethod

class BaseReader(ABC):
    @abstractmethod
    def yield_chunks(self):
        """
        Must be implemented by all child dataset readers. Should yield 10-second chunks of data as ECGRecord objects.
        """
        pass