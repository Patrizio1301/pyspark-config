from main.process.input.sources.csv import Csv
from main.process.input.sources.parquet import Parquet


class Constructor(object):
    @staticmethod
    def apply(config):
        if 'type' in config.keys():
            type=config['type']
            if type == 'csv':
                return Csv().from_dict(config)
            if type == 'parquet':
                return Parquet().from_dict(config)
        else:
            print("ERROR")