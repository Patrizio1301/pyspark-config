""" Transformation Testing """

from pyspark.sql.types import *
import pyspark.sql.functions as F

from pyspark_config.transformations import *
from tests.python.pyspark_utils import PySparkTestCase
from pyspark_config.transformations.transformations import Aggregation


def getType(schema: StructType, col: str) -> DataType:
    fields=[field for field in schema.fields if field.name == col]
    if len(fields)==0:
        raise AttributeError("No column with name %s available." %(col))
    elif len(fields)==1:
        return next(iter(fields)).dataType
    else:
        raise AttributeError("Ambiguous columns with name %s." %(col))


class Base64Transformations(PySparkTestCase):

    def testCastIntToString(self):
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Base64(
            col='age', colName='age_str',
        ).transform(df, self.sc._jvm)

        df_result.show()



class CastTransformations(PySparkTestCase):

    def testCastIntToString(self):
        """
        Test if column of IntegerType can be transformed to StringType.
        """
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_str', fromType='int', toType='string'
        ).transform(df)

        df_expected = self.spark.createDataFrame([(1,'1')], ['age', 'age_str'])

        self.assertIsInstance(getType(df_result.schema, 'age_str'), StringType)
        self.assertEqual(df_result.collect(), df_expected.collect())

    def testCastIntToFloat(self):
        """
        Test if column of IntegerType can be transformed to FloatType.
        """
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_float', fromType='int', toType='float'
        ).transform(df)

        df_expected = self.spark.createDataFrame([(1,float(1))], ['age', 'age_float'])

        self.assertIsInstance(getType(df_result.schema, 'age_float'), FloatType)
        self.assertEqual(df_result.collect(), df_expected.collect())

    def testCastIntToLong(self):
        """
        Test if column of IntegerType can be transformed to LongType.
        """
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_long', fromType='int', toType='long'
        ).transform(df)

        df_expected = self.spark.createDataFrame([(1,int(1.0))], ['age', 'age_long'])

        self.assertIsInstance(getType(df_result.schema, 'age_long'), LongType)
        self.assertEqual(df_result.collect(), df_expected.collect())

    def testCastIntToDouble(self):
        """
        Test if column of IntegerType can be transformed to DoubleType.
        """
        df = self.spark.createDataFrame([(1,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_long', fromType='int', toType='double'
        ).transform(df)

        df_expected = self.spark.createDataFrame([(1,float(1.0))], ['age', 'age_double'])

        self.assertIsInstance(getType(df_result.schema, 'age_long'), DoubleType)
        self.assertEqual(df_result.collect(), df_expected.collect())

    def testCastIntToBoolean(self):
        """
        Test if column of IntegerType can be transformed to LongType.
        """
        df = self.spark.createDataFrame([(1,), (0,)], ['age'])
        df_result = Cast(
            col='age', castedCol='age_bool', fromType='int', toType='boolean'
        ).transform(df)

        df_expected = self.spark.createDataFrame(
            [(1,True), (0, False)], ['age', 'age_bool'])

        self.assertIsInstance(getType(df_result.schema, 'age_bool'), BooleanType)
        self.assertEqual(df_result.collect(), df_expected.collect())

    def testCastStringToInt(self):
        """
        Test if column of StringType can be transformed to IntegerType.
        """
        df = self.spark.createDataFrame([('1',)], ['age_str'])
        df_result = Cast(
            col='age_str', castedCol='age', fromType='string', toType='int'
        ).transform(df)

        df_expected = self.spark.createDataFrame([('1', 1)], ['age_str', 'age'])

        self.assertIsInstance(getType(df_result.schema, 'age'), IntegerType)
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