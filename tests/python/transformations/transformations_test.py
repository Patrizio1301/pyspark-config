""" Transformation Testing """

import unittest
from pyspark_config.config import Config
from pyspark_config.errors import *
from pyspark_config.input import *
from pyspark_config.yamlConfig.config import getSubclass
from tests.resource.constants import SOURCES
from pyspark_config.transformations import *


class CastTransformations(unittest.TestCase):

    def testCastIntToString(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        Cast('age', 'ages', 'int', 'string').transform(df).collect()

        configuration=Config()

        self.assertEqual(None, configuration.load(conf_avble))
        self.assertRaises(NotFoundError, configuration.load, conf_not_avble)
        self.assertRaises(InvalidTypeError, configuration.load, conf_wrong_type)