from dataclasses import dataclass
from typing import List
from main.process.transformations.transformation import Transformation
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
from main.YamlConfig.config import dataclass_json


@dataclass_json
@dataclass
class Output:
    type: str = None
    name: str = None
    path: str = None
    transformations: List[Transformation] = None

    def apply(self, df: DataFrame_Extended):
        pass