from dataclasses import dataclass
from main.process.input.sources.source import Source
from spark_utils import DataFrame_Extended

@dataclass
class Parquet(Source):
    parquet_path: str=None

    @classmethod
    def get_from_config(cls, config):
        return cls(
            type=config['type'],
            path=config['path'] if 'path' in config.keys() else None,
            label=config['label'],
            parquet_path = config['parquet_path']
        )

    def apply(self, spark_session):
        input=spark_session.read.parquet(self.parquet_path)
        input=DataFrame_Extended(
            df=input,
            spark_session=spark_session
        )
        return self.transformations(df=input)




