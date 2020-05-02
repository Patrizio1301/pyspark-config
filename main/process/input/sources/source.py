from dataclasses import dataclass
from main.process.transformations.functions.transformations import Transformation_List
from main.process.transformations.transformation import Transformations
from main.YamlConfig.config import dataclass_json


@dataclass_json
@dataclass
class Source:
    type: str =None
    label: str =None
    path: str = None

    def apply(self, spark_session):
        pass