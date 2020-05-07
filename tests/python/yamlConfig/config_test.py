""" yamlConfig Testing """

import unittest
from pyspark_config.config import Config
from pyspark_config.errors import *
from pyspark_config.input import *
from pyspark_config.yamlConfig.config import getSubclass
from tests.resource.constants import SOURCES


class TestGetSubclass(unittest.TestCase):

    def test_getSubclass(self):
        # TODO Add test for getSubclass
        #getSubclass(Source,SOURCES.CSV_SOURCE
        pass


class TestYamlDataClassConfig(unittest.TestCase):

    def test_fileHandling(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        FOLDER='../../resource/yamlConfig/'
        conf_avble='%stest_fileHandling_conf.yaml'%(FOLDER)
        conf_not_avble='%stest_fileHandling_conf_wrong.yaml'%(FOLDER)
        conf_wrong_type='%stest_fileHandling_conf.txt'%(FOLDER)

        configuration=Config()

        self.assertEqual(None, configuration.load(conf_avble))
        self.assertRaises(NotFoundError, configuration.load, conf_not_avble)
        self.assertRaises(InvalidTypeError, configuration.load, conf_wrong_type)