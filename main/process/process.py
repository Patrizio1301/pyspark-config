from dataclasses import dataclass

from main.process.configuration.input_config_reader import InputConfig
from main.process.configuration.transformation_config_reader import TransformationConfig
from main.process.configuration.output_config_reader import OutputConfig
from main.process.configuration.config_reader import ConfigReader
from main.process.input.input import Input
from main.process.transformations.transformation import Transformations
from main.process.transformations.functions.transformations import Transformation_List
from main.process.output.constructor import Constructor
from process.dfo import DFO

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Process(object):
    yaml_path: str
    label: str
    inputs: DFO=None

    @property
    def config(self):
        return ConfigReader(yaml_path=self.yaml_path)

    @property
    def input(self):
        """
        Input dataFrame generated and configurated by
        the input configuration object.

        """
        return Input.get_from_config(
            config=InputConfig.get_input_config(
                config=self.config.configuration)
        ).apply(
            spark_session=self.config.spark_session
        )

    @property
    def transformations(self):
        """
        Transformed dataFrame generated by applying
        the transformations configured in the transformation
        configuration object to the input dataframe.

        """
        return Transformations(
            config=TransformationConfig.get_transformation_config(
                config=self.config.configuration
            ),
            transformation_cls=Transformation_List
        ).apply(
            df=self.input
        )

    @property
    def output(self):
        """
        Output generated by applying the output configuration
        object to the transformed dataFrame. Since multiple
        outputs are written

        """

        for config in OutputConfig.get_output_config(
                config=self.config.configuration):
            Constructor.get_from_config(
                config=config
            ).apply(
                df=self.transformations
            )
        return logger.info('The process has been '
                           'finished sucessfully.')