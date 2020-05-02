from pathlib import Path
import unittest
from main.process.config import Config
from main.process.output.types.output import Output


class testConfigurableTestCase(unittest.TestCase):
    def setUp(self):
        inp=Config()
        #inp.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/input.yaml'))
        #print(inp.input.__dict__)

    def test_input(yaml_config):
        parquet = {'type': 'Parquet', 'label': 'events', 'path': '/.../test.parquet',
                   'parquet_path': '../../result.parquet'}
        csv = {'type': 'Csv', 'label': 'events', 'path': '/.../test.csv', 'csv_path': '../../result.csv',
               'delimiter': ','}
        input = {'sources': [csv, parquet]}
        #Input.from_dict(input)

    def test_output(yaml_config):
        inp = Config()
        print(Output.__subclasses__())
        inp.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/output.yaml'))

        print(inp)

