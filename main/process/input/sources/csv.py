from dataclasses import dataclass
from main.process.input.sources.source import Source
from spark_utils import DataFrame_Extended

@dataclass
class Csv(Source):
    csv_path: str=None
    delimiter: str=';'

    @classmethod
    def get_from_config(cls, config):
        return cls(
            type=config['type'],
            path=config['path'] if 'path' in config.keys() else None,
            label=config['label'],
            csv_path = config['csv_path'],
            delimiter = config['delimiter']
        )

    def apply(self, spark_session):
        input=spark_session.read.option("delimiter", self.delimiter).csv(
           self.csv_path, header=True, mode="DROPMALFORMED"
        )
        input=DataFrame_Extended(
            df=input,
            spark_session=spark_session
        )
        return self.transformations(df=input)




