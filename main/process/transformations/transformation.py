from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
from dataclasses import dataclass
from typing import List
from main.process.transformations.functions.Transformations.transformations import Transformation
from main.YamlConfig.config import dataclass_json


class Transformations(object):
    def __init__(self, transformation_cls, config: List[Transformation]):
        self.transformation_cls = transformation_cls
        self.config=config

    def apply(self, df: DataFrame_Extended):
        """
        Apply multiple transformations to a dataFrame determined
        by the transformation configuration indicated in the `config`
        attribute of this class.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformations.

        """
        for trans in self.config:
            df=trans.apply_transformation(
                df=df,
                transformation_cls=self.transformation_cls
            )
        return df




