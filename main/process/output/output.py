from dataclasses import dataclass
from typing import List
from main.process.transformations.transformation import Transformation
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Output:
    type: str
    name: str
    path: str
    transformations: List[Transformation]

    @classmethod
    def get_from_config(cls, config):
        type=config['type']
        name=config['name']
        path=config['path']
        transformations=[Transformation.get_from_config(cfg)
                         for cfg in config['transformations']]
        return cls(
            type=type,
            name=name,
            path=path,
            transformations=transformations
        )

    def apply(self, df: DataFrame_Extended):
        pass