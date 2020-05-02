from dataclasses import dataclass
from typing import List
from main.process.input.input import Input
from main.process.transformations.transformation import Transformation
from main.process.output.types.output import Output
from main.YamlConfig.config import YamlDataClassConfig
from main.process.output.types.csv import Csv
from main.process.output.types.parquet import Parquet
from main.process.output.types.json import Json


@dataclass
class Config(YamlDataClassConfig):
    input: Input =None
    output: List[Output] =None
    transformation: List[Transformation] =None