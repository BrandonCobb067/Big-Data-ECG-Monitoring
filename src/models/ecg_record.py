# This file represents the universal data structure so that downstream systems like Apache Spark and MongoDB remain agnostic to the original .csv, .dat, or .mat file formats. 

class ECGRecord:
    def __init__ (self, record_id, sample_rate, leads, units, samples):
        self.record_id = record_id
        self.sample_rate = sample_rate
        self.leads = leads
        self.units = units
        self.samples = samples