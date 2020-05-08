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

        df_expected = self.spark.createDataFrame([(1,"1")], ['age', 'age_str'])

        self.assertEqual(df_result.collect(), df_expected.collect())


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

        df_expected = self.spark.createDataFrame(
            [(sp, ['Seville','Bilbao', 'Madrid']),
             (usa, ['New York', 'Washington']),
             (ger,['Aachen','Hamburg', 'Berlin'])],
            ['country', 'city']
        )

        self.assertEqual(df_result.collect(), df_expected.collect())


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

        df_expected = self.spark.createDataFrame(
            [('Max','Muster', 'Max-Muster')], ['name', 'surname', 'concat'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class DayOfMonthTransformation(PySparkTestCase):

    def testDayOfMonth(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfMonth(date='dt', colName='dayOfMonth').transform(df)

        df_expected = self.spark.createDataFrame(
            [('2015-04-08',3)], ['dt', 'dayOfMonth'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class DayOfYearTransformation(PySparkTestCase):

    def testDayOfYear(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfYear(date='dt', colName='DayOfYear').transform(df)

        df_expected = self.spark.createDataFrame(
            [('2015-04-08',97)], ['dt', 'DayOfYear'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class DayOfWeekTransformation(PySparkTestCase):

    def testDayOfWeek(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = DayOfWeek(date='dt', colName='DayOfWeek').transform(df)

        df_expected = self.spark.createDataFrame(
            [('2015-04-08',3)], ['dt', 'DayOfWeek'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class FilterTransformation(PySparkTestCase):

    def testFilter(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(2,'Alice'), (5, 'Bob')], ['age', 'name'])
        df_result = Filter(sql_condition='age>3').transform(df)

        df_expected = self.spark.createDataFrame([(5, 'Bob')], ['age', 'name'])

        self.assertEqual(df_result.collect(), df_expected.collect())


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

        df_expected=self.spark.createDataFrame([(5, 'Bob'), (7, 'Max')], ['age', 'name'])

        self.assertEqual(df_result.collect(), df_expected.collect())


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

        df_expected = self.spark.createDataFrame([(5, 'Bob'), (7, 'Max')], ['age', 'name'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class ListLengthTransformation(PySparkTestCase):

    def testListLength(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([([1,2,4,2],'Alice'), ([3,4,5], 'Bob')], ['notes', 'name'])
        df_result = ListLength(col='notes', colName='num_notes').transform(df)

        df_expected = self.spark.createDataFrame(
            [([1,2,4,2],'Alice', 4), ([3,4,5], 'Bob', 3)], ['notes', 'name', 'num_notes'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class MonthTransformation(PySparkTestCase):

    def testMonth(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result=Month(date='dt', colName='month').transform(df)

        df_expected = self.spark.createDataFrame(
            [('2015-04-08',3)], ['dt', 'month'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class NormalizationTransformation(PySparkTestCase):

    def testNormalization(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(1,), (3,), (5,)], ['amount'])
        df_result=Normalization(
            col='amount', colName='amount_normalized'
        ).transform(df)

        df_expected = self.spark.createDataFrame(
            [(1, 0), (3,0.5), (5, 1.0)], ['amount', 'amount_normalized'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class PercentageTransformation(PySparkTestCase):

    def testPercentage(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([(4,), (2,), (4,)], ['amount'])
        df_result=Percentage(
            col='amount', colName='amount_percentaje'
        ).transform(df)

        df_expected = self.spark.createDataFrame(
            [(4,0.4), (2,0.2), (4,0.4)], ['amount', 'amount_percentaje'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class SelectTransformation(PySparkTestCase):

    def testSelect(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('Bob',1), ('Ana',2), ('Max',3)], ['name', 'age'])
        df_result=Select(
            cols=['age']
        ).transform(df)

        df_expected=self.spark.createDataFrame([(1,), (2,), (3,)], ['age'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class SortByTransformation(PySparkTestCase):

    def testSortBy(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame(
            [(2,'Alice'), (7, 'Bob'), (5, 'Bob')], ['age', 'name'])

        df_result = SortBy(
            col="age", ascending=False
        ).transform(df)

        df_expected = self.spark.createDataFrame(
            [(7, 'Bob'), (5, 'Bob'), (2, 'Alice')], ['age', 'name'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class SplitTransformation(PySparkTestCase):

    def testSplit(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('ab12cd',)], ['s', ])
        df_result = Split(
            col='s', colName='splitted',  delimiter='[0-9]+'
        ).transform(df)

        df_expected = self.spark.createDataFrame(
            [('ab12cd', ['ab', 'cd'])], ['s', 'splitted'])

        self.assertEqual(df_result.collect(), df_expected.collect())


class YearTransformation(PySparkTestCase):

    def testYear(self):
        """
        Test if exception is raised, when file does not exist
        Test if no exception is raised when file exists
        Test if exception is raised when file type is not yaml
        """
        df = self.spark.createDataFrame([('2015-04-08',)], ['dt'])
        df_result = Year(date='dt', colName='year').transform(df)

        df_expected = self.spark.createDataFrame(
            [('2015-04-08',2015)], ['dt', 'year'])

        self.assertEqual(df_result.collect(), df_expected.collect())