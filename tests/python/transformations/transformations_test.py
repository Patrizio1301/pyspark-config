""" Transformation Testing """

import unittest
from pyspark_config.config import Config
from pyspark_config.errors import *
from pyspark_config.input import *
from pyspark_config.yamlConfig.config import getSubclass
from tests.resource.constants import SOURCES
from pyspark_config.transformations import *
from tests.python.pyspark_utils import PySparkTestCase
from pyspark_config.transformations.transformations import Aggregation


class CastTransformations(PySparkTestCase):

    def testCastIntToString(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_str', fromType='int', toType='string'
        ).transform(df)


class CollectListTransformation(PySparkTestCase):

    def testCollectList(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        ger = 'GERMANY'
        sp = 'SPAIN'
        usa = 'UNITED STATES'
        df = self.spark.createDataFrame(
            [(ger,'Aachen'), (ger,'Hamburg'), (sp,'Seville'), (usa,'New York'),
             (ger,'Berlin'), (usa,'Washington'), (sp,'Bilbao'), (sp,'Madrid')],
            ['country', 'city']
        )
        df_result =  CollectList(
            orderBy=['country'], groupBy=['country'], cols=['city']
        ).transform(df)

        df_result.show()


class ConcatenateTransformation(PySparkTestCase):

    def testConcatenate(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('Max','Muster')], ['name', 'surname'])
        df_result =  Concatenate(
            cols=['name', 'surname'], name='concat', delimiter='-'
        ).transform(df)

        df_result.show()


class DayOfMonthTransformation(PySparkTestCase):

    def testDayOfMonth(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfMonth(date='dt', colName='dayOfMonth').transform(df)

        df_result.show()


class DayOfYearTransformation(PySparkTestCase):

    def testDayOfYear(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfYear(date='dt', colName='DayOfYear').transform(df)

        df_result.show()


class DayOfWeekTransformation(PySparkTestCase):

    def testDayOfWeek(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfWeek(date='dt', colName='DayOfWeek').transform(df)

        df_result.show()


class FilterTransformation(PySparkTestCase):

    def testFilter(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(2,'Alice'), (5, 'Bob')], ['age', 'name'])
        df_result = Filter(sql_condition='age>3').transform(df)

        df_result.show()


class FilterByListTransformation(PySparkTestCase):

    def testFilterByList(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame(
            [(2,'Alice'), (5, 'Bob'), (7, 'Max')], ['age', 'name'])
        df_result = FilterByList(
            col='name', values=['Bob', 'Max']
        ).transform(df)

        df_result.show()


class GroupByTransformation(PySparkTestCase):

    def testGroupBy(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(2,'Alice'), (5, 'Bob'), (5, 'Bob')], ['age', 'name'])
        df_result = GroupBy(
            groupBy = ['name'],
            aggregations = [Aggregation('sum', ['age']), Aggregation('mean', ['age'])]
        ).transform(df)

        df_result.show()


class ListLengthTransformation(PySparkTestCase):

    def testListLength(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([([1,2,4,2],'Alice'), ([3,4,5], 'Bob')], ['notes', 'name'])
        df_result = ListLength(col='notes', colName='num_notes').transform(df)

        df_result.show()


class MonthTransformation(PySparkTestCase):

    def testMonth(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result=Month(date='dt', colName='month').transform(df)

        df_result.show()


class MonthTransformation(PySparkTestCase):

    def testMonth(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result=Month(date='dt', colName='month').transform(df)

        df_result.show()