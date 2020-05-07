import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import unittest
from pyspark_config.input.input import Input
from pyspark_config.output import Json, Csv, Parquet
from pyspark_config.transformations import Filter, Select


class inputTestCase(unittest.TestCase):
    def setUp(self):
        spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()
        df=spark.createDataFrame([(1, 'foo'), (2, 'bar'),],['A', 'B'])

    @staticmethod
    def testJson():
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
        df = spark.createDataFrame([(1, 'foo'), (2, 'bar'), ], ['A', 'B'])
        Json(
            name='test_json',
            path='/home/patrizio/PycharmProjects/pyspark-config/tests/resource/output_path'
        ).apply(df)

    @staticmethod
    def test_Csv():
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
        df = spark.createDataFrame([(1, 'foo'), (2, 'bar'), ], ['A', 'B'])
        Csv(
            name='test_csv',
            path='/home/patrizio/PycharmProjects/pyspark-config/tests/resource/output_path',
        ).save(df)



    @staticmethod
    def test_Csv_with_trans():
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
        df = spark.createDataFrame([(1, 'foo', 3), (2, 'bar', 4), ], ['D', 'E', 'F'])
        Csv(
            name='test_csv',
            path='/home/patrizio/PycharmProjects/pyspark-config/tests/resource/output_path',
            transformations=[
                Select(
                    cols=["D", "E", "F"]
                )
            ]
        ).save(df)


