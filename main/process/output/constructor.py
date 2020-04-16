from main.process.output.types.csv import Csv
from main.process.output.types.json import Json
from main.process.output.types.parquet import Parquet

class Constructor(object):

    @staticmethod
    def get_from_config(config):
        type=config['type']
        if type=='csv':  return Csv.get_from_config(config)
        elif type == 'json':  return Json.get_from_config(config)
        elif type == 'parquet':  return Parquet.get_from_config(config)
