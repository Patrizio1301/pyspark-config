from main.process.input.sources.lake import Lake
from main.process.input.sources.csv import Csv
from main.process.input.sources.parquet import Parquet

class Constructor(object):

    @staticmethod
    def get_from_config(config):
        type=config['type']
        if type=='lake':  return Lake.get_from_config(config)
        if type == 'csv':  return Csv.get_from_config(config)
        if type == 'parquet':  return Parquet.get_from_config(config)