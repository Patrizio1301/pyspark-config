from dataclasses import dataclass
from dataclasses_json import dataclass_json
from main.process.input.sources.source import Source
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended


@dataclass_json
@dataclass
class Csv(Source):
    type="Csv"
    csv_path: str=None
    delimiter: str=';'

    def apply(self, spark_session):
        input=spark_session.read.option("delimiter", self.delimiter).csv(
           self.csv_path, header=True, mode="DROPMALFORMED"
        )
        input=DataFrame_Extended(
            df=input,
            spark_session=spark_session
        )
        return self.transformations(df=input)




