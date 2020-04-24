from dataclasses import dataclass
from main.process.configuration.config_reader import ConfigReader
from main.process.configuration.transformation_config_reader import TransformationConfig
from main.process.transformations.functions.transformations import Transformation_List
from main.process.transformations.transformation import Transformations
from dataclasses_json import dataclass_json
from dataclasses_json import DataClassJsonMixin

@dataclass_json
@dataclass
class Source(object):
    type: str =None
    label: str =None
    path: str = None

    def __new__(cls, type, label, path, *args, **kwargs):
        subclass_map = {subclass.type: subclass for subclass in cls.__subclasses__()}
        subclass = subclass_map[type]
        instance = object.__new__(subclass)
        print(instance.__class__)
        return instance

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