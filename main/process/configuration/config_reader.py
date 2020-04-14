import yaml
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from src.spark_utils.spark_session.sparkSession import arranca_spark

class ConfigReader(object):
    def __init__(self, yaml_path: str, spark_session=None):
        self.spark_session_=spark_session
        self.yaml_path = yaml_path

    @property
    def spark_session(self):
        """
        Spark session used during the process.
        """
        if self.spark_session_:
            return self.spark_session_
        else:
            session_name=self.configuration["session_name"]
            return arranca_spark(session_name)

    @property
    def configuration(self):
        """
        Configuration of the process. Parse the first YAML
        document in a stream and produce the corresponding
        Python object.

        :return: configuration dictionary
        """
        with open(self.yaml_path, 'r') as ymlfile:
            return yaml.load(ymlfile)







