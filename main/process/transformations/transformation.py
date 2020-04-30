from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
from dataclasses import dataclass
from typing import List
from main.YamlConfig.config import dataclass_json


@dataclass_json
@dataclass
class Transformation:
    type: str
    attributes: dict

    @classmethod
    def get_from_config(cls, config):
        return cls(
            type=config['type'],
            attributes=config['attributes']
        )

    def apply_transformation(self, df, transformation_cls):
        """
        Apply unique transformation to a DataFrame

        :param: df: class:`DataFrame_Extended`
            Input dataFrame
        :param: type: str
            Indicates the method to apply to df.
        :param: attributes: json object.
            Attributes applied to the method determined in `type`.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformation.

        """
        return getattr(transformation_cls, self.type)(df, **self.attributes)


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




