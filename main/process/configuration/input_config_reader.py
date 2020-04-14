import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InputConfig(object):
    def __init__(self, config, spark_session=None):
        self.spark_session_=spark_session
        self.config = config

    @staticmethod
    def get_input_config(config):
        """
        Configuration for the input handling.

        :params: config containing an attribute 'input'

        :return: input configuration.

        """
        try:
            return config['input']
        except KeyError:
            raise KeyError('The configuration must contain '
                           'the attribute "input".')
        except Exception as e:
            logger.exception(e)



