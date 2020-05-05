import unittest
from pyspark_config import Config
from pathlib import Path
from pyspark_config.input import Csv
from pyspark_config.input import Parquet

from pyspark_config.input.creator import Join

from pyspark_config.transformations import *
from pyspark_config.output import Parquet



class inputTestCase(unittest.TestCase):

    def test_multiple(self):
        conf = Config()
        conf.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/input_test.yaml'))
        conf.input.table_dict(conf.spark_session)
        conf.input.get_input(conf.spark_session)
        conf.apply()
        print(conf)



