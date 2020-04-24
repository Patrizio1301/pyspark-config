from dataclasses import dataclass
from dataclasses_json import dataclass_json
from main.process.input.sources.source import Source
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended


@dataclass_json
@dataclass
class Parquet(Source):
    type= "Parquet"
    parquet_path: str=None

    def apply(self, spark_session):
        input=spark_session.read.parquet(self.parquet_path)
        input=DataFrame_Extended(
            df=input,
            spark_session=spark_session
        )
        return self.transformations(df=input)




