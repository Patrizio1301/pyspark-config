from spark_utils import DataFrame_Extended
from typing import List

class Transformation_List(object):

    @staticmethod
    def select(
            df: DataFrame_Extended,
            cols: List[str]
    ) -> DataFrame_Extended:
        return df.select(cols)

    @staticmethod
    def filter(
            df: DataFrame_Extended,
            sql_condition: str
    ) -> DataFrame_Extended:
        return df.filter(sql_condition)

    @staticmethod
    def filter_by_list(
            df: DataFrame_Extended,
            col: str,
            choice_list:List[str]
    ) -> DataFrame_Extended:
        return df.filter_by_list(col=col, choice_list=choice_list)

    @staticmethod
    def cast(
            df: DataFrame_Extended,
            col: str,
            newCol: str,
            fromType: str,
            toType: str
    ) -> DataFrame_Extended:
        return df.cast(col=col, newCol=newCol, fromType=fromType, toType=toType)

    @staticmethod
    def normalization(
            df: DataFrame_Extended,
            col: str,
            newCol: str
    ) -> DataFrame_Extended:
        return df.normalization(col=col, newCol=newCol)

    @staticmethod
    def sort_by(
            df: DataFrame_Extended,
            column: str,
            ascending=False
    ) -> DataFrame_Extended:
        return df.sort(column, ascending=ascending)

    @staticmethod
    def groupby(
            df: DataFrame_Extended,
            groupBy_col_list,
            sum_col_list: List[str] = [],
            count_col_list:List[str]=[]
    ) -> DataFrame_Extended:
        return df.groupby(
            groupBy_col_list=groupBy_col_list,
            sum_col_list=sum_col_list,
            count_col_list=count_col_list
        )

    @staticmethod
    def concatenate(
            df: DataFrame_Extended,
            cols: List[str],
            name: str,
            delimiter:str=""
    ) -> DataFrame_Extended:
        return df.concatenate(cols, name, delimiter)

    @staticmethod
    def split(df, column, newCol, delimiter) -> DataFrame_Extended:
        return df.split(column, newCol, delimiter)

    @staticmethod
    def applyFastText(df: DataFrame_Extended,delimiter,originalCol,column,newCol,size,window,min_count,epoche) -> DataFrame_Extended:
        return df.applyFastText(delimiter,originalCol,column,newCol,size,window,min_count,epoche)

    @staticmethod
    def add_perc(
            df: DataFrame_Extended,
            column: str,
            perc_name: str="perc"
    ) -> DataFrame_Extended:
        return df.add_perc(column, perc_name)

    @staticmethod
    def add_date(
            df: DataFrame_Extended,
            date: str
    ) -> DataFrame_Extended:
        return df.add_date(date)

    @staticmethod
    def collect_list(
            df: DataFrame_Extended,
            order_by,
            group_by_list,
            column_list
    ) -> DataFrame_Extended:
        return df.collect_list(
            order_by=order_by,
            group_by_list=group_by_list,
            column_list=column_list
        )

    @staticmethod
    def list_length(
            df: DataFrame_Extended,
            column: str
    ) -> DataFrame_Extended:
        return df.list_length(
            column
        )

    @staticmethod
    def one_hot_encoder(
        df: DataFrame_Extended,
        col: str,
        newCol: str,
        vocabSize: int
    ) -> DataFrame_Extended:
        return df.one_hot_encoder(
            col=col,
            newCol=newCol,
            vocabSize=vocabSize
        )

    @staticmethod
    def cluster_df(
            df: DataFrame_Extended,
            cluster_col:str,
            cluster_list:List[str],
            groupby_col_list: List[str]=[],
            sum_col_list: List[str]=[],
            count_col_list: List[str]=[]
    ) -> DataFrame_Extended:
        return df.cluster_df(
            cluster_col=cluster_col,
            cluster_list=cluster_list,
            groupby_col_list=groupby_col_list,
            sum_col_list=sum_col_list,
            count_col_list=count_col_list
        )



