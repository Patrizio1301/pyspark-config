from pyspark.sql.dataframe import DataFrame
from src.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
from dataclasses import dataclass

@dataclass
class DFO(object):
    df: DataFrame_Extended
    label: str

