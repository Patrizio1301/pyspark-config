from pathlib import Path
import unittest
from pyspark_config.config import Config
from pyspark_config.input.input import Input
from pyspark_config.transformations.transformations import *
from pyspark_config.output import Json
from pyspark_config.output import Parquet as Parquet_Output
from pyspark_config.output import Csv as Csv_output

from pyspark_config.input import Csv
from pyspark_config.input import Parquet



class testConfigurableTestCase(unittest.TestCase):

    def test_input(self):
        conf = Config()
        conf.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/input.yaml'))
        expected=Config(
            input=Input(
                sources=
                [
                    Parquet(
                        type='Parquet',
                        label='events',
                        path='heloooo.parquet',
                        parquet_path='heloooo.pacucucu'),
                    Csv(
                        type='Csv',
                        label='events',
                        path='heloooo.csv',
                        csv_path='heloooo.csucsu',
                        delimiter='demiliiiiii')
                ],
                transformations=[
                    Split(
                        type='Split',
                        column='A',
                        newCol='new',
                        delimiter=','
                    )
                ]
            ),
            output=None,
            transformation=None
        )
        self.assertEqual(conf, expected)

    def test_output(self):
        conf = Config()
        conf.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/output.yaml'))
        expected=Config(
            input=None,
            output=
            [
                Csv_output(
                    type='Csv',
                    name='outputi',
                    path='ksandkjanskda',
                    transformations=None,
                    delimiter=','),
                Json(
                    type='Json',
                    name='outputi',
                    path='ksandkjanskda',
                    transformations=None),
                Parquet_Output(
                    type='Parquet',
                    name='outputi',
                    path='ksandkjanskda',
                    transformations=None,
                    partitionCols=['hiJN', 'ADSAD'])
            ],
            transformation=None
        )
        self.assertEqual(conf, expected)


    def test_transformations(self):
        conf = Config()
        conf.load(Path('/home/patrizio/PycharmProjects/pyspark-config/tests/resource/configuration_tests/transformations.yaml'))
        expected=Config(
            input=None,
            output=None,
            transformations=[
                Select(
                    type='Select',
                    cols='sql_condition'
                ),
                Filter(
                    type='Filter',
                    sql_condition=['A', 'B', 'C']
                )
            ]
        )
        self.assertEqual(conf, expected)



