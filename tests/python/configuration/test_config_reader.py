from pathlib import Path
import unittest
import yaml
from main.process.input.input import Input
from main.process.config import Config


class testConfigurableTestCase(unittest.TestCase):
    def setUp(self):
        inp=Config()
        inp.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/input.yaml'))
        print(inp.input.__dict__)

    def test_something(yaml_config):
        print("Hello")

