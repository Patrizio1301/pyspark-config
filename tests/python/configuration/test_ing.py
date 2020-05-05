from pathlib import Path
import unittest
from pyspark_config.config import Config


from pyspark_config.input.input import Input

from pyspark_config.transformations.transformations import *
from pyspark_config.output import *
from pyspark_config.input import *



conf = Config()
conf.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/testing.yaml'))
conf.apply()
print(conf)