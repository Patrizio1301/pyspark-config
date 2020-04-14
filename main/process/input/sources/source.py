from dataclasses import dataclass
from sanformer.main.process.configuration.config_reader import ConfigReader
from sanformer.main.process.configuration.transformation_config_reader import TransformationConfig
from spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
from sanformer.main.process.transformations.functions.transformations import Transformation_List
from sanformer.main.process.transformations.transformation import Transformations

@dataclass
class Source:
    type: str
    label: str
    path: str = None

    @staticmethod
    def get_from_config(config):
        pass

    def apply(self, spark_session):
        pass

    def transformations(self, df):
        if self.path:
            config=ConfigReader(yaml_path=self.path)
            return Transformations(
                config=TransformationConfig.get_transformation_config(
                    config=config.configuration
                ),
                transformation_cls=Transformation_List
            ).apply(
                df=df
            )
        else:
            return df