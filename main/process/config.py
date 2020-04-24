from dataclasses import dataclass
from main.process.input.input import Input
from main.process.transformations.transformation import Transformation
from main.process.output.output import Output
from main.YamlConfig.config import YamlDataClassConfig


@dataclass
class Config(YamlDataClassConfig):
    input: Input =None
    output: Output =None
    transformation: Transformation =None