from dataclasses import dataclass
from typing import List
from pyspark_config.input.input import Input
from pyspark_config.transformations.transformations import Transformation
from pyspark_config.output.output import Output
from pyspark_config.yamlConfig.config import YamlDataClassConfig
from pyspark.sql import SparkSession
from pyspark_config.spark_utils.dataframe_extended import DataFrame_Extended


@dataclass
class Config(YamlDataClassConfig):
    input: Input =None
    transformations: List[Transformation] = None
    output: List[Output] =None

    @property
    def spark_session(self):
        return SparkSession.builder\
            .master("local") \
            .appName("Word Count") \
            .enableHiveSupport() \
            .getOrCreate()

    def __apply_input__(self):
        """
        Apply multiple transformations to a dataFrame determined
        by the transformation configuration indicated in the `config`
        attribute of this class.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformations.

        """
        return self.input.get_input(self.spark_session)

    def __apply_transformations__(self, df: DataFrame_Extended):
        """
        Apply multiple transformations to a dataFrame determined
        by the transformation configuration indicated in the `config`
        attribute of this class.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformations.

        """
        for trans in self.transformations:
            df=trans.transform(df=df)
        return df

    def __get_outputs__(self, df):
        """
        Apply multiple transformations to a dataFrame determined
        by the transformation configuration indicated in the `config`
        attribute of this class.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformations.

        """
        for output in self.output:
            output.save(df)
        return None

    def apply(self):
        """
        Apply multiple transformations to a dataFrame determined
        by the transformation configuration indicated in the `config`
        attribute of this class.

        :return: class:`DataFrame_Extended`. DataFrame as an output
        of the performed transformations.

        """
        df = DataFrame_Extended(self.__apply_input__(), self.spark_session)
        df_transformed = self.__apply_transformations__(df)
        self.__get_outputs__(df_transformed)