import yaml
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OutputConfig(object):
    def __init__(self, config, spark_session=None):
        self.spark_session_=spark_session
        self.config = config

    @staticmethod
    def get_output_config(config):
        """
        Configuration for the output handling.

        :params: config containing an attribute 'output'

        :return: output configuration.

        """
        try:
            return config['output']
        except KeyError:
            raise KeyError('The configuration must contain '
                           'the attribute "output".')
        except Exception as e:
            logger.exception(e)



