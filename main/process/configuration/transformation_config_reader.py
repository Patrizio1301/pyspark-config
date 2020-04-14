import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from sanformer.main.process.transformations.transformation import Transformation

class TransformationConfig(object):
    def __init__(self, config, spark_session=None):
        self.spark_session_=spark_session
        self.config = config

    @staticmethod
    def get_transformation_config(config):
        """
        Configuration for the input handling.

        :params: config containing an attribute 'transformations'

        :return: transformation configuration.

        """
        try:
            return [Transformation.get_from_config(cfg)
                    for cfg in config['transformations']]
        except Exception as e:
            logger.exception(e)


