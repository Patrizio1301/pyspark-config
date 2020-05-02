from main.process.transformations.functions.transformation import Transformation
from typing import List
from dataclasses import dataclass

from main.YamlConfig.config import dataclass_json
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended


@dataclass_json
@dataclass
class Select(Transformation):
    type = "Select"
    df: DataFrame_Extended = None
    sql_condition: str = None

    def transform(self):
        return self.df.filter(self.sql_condition)


@dataclass_json
@dataclass
class Filter(Transformation):
    type = "Filter"
    df: DataFrame_Extended = None
    cols: List[str] = None

    def transform(self):
        return self.df.select(self.cols)


@dataclass_json
@dataclass
class FilterByList(Transformation):
    type = "FilterByList"
    df: DataFrame_Extended = None
    col: str = None
    choice_list:List[str] = None

    def transform(self):
        return self.df.filter_by_list(
            col=self.col,
            choice_list=self.choice_list
        )


@dataclass_json
@dataclass
class Cast(Transformation):
    type = "Cast"
    df: DataFrame_Extended = None
    col: str = None
    newCol: str = None
    fromType: str = None
    toType: str = None

    def transform(self):
        return self.df.cast(
            col=self.col,
            newCol=self.newCol,
            fromType=self.fromType,
            toType=self.toType)


@dataclass_json
@dataclass
class Normalization(Transformation):
    type = "Normalization"
    df: DataFrame_Extended = None
    col: str = None
    newCol: str = None

    def transform(self):
        return self.df.normalization(
            col=self.col,
            newCol=self.newCol)


@dataclass_json
@dataclass
class SortBy(Transformation):
    type = "SortBy"
    df: DataFrame_Extended = None
    column: str = None
    ascending: bool = False

    def transform(self):
        return self.df.sort(
            self.column,
            ascending=self.ascending)


@dataclass_json
@dataclass
class GroupBy(Transformation):
    type = "GroupBy"
    df: DataFrame_Extended = None
    groupBy_col_list: List[str] = None
    sum_col_list: List[str] = None
    count_col_list:List[str]= None

    def transform(self):
        return self.df.groupby(
            groupBy_col_list=self.groupBy_col_list,
            sum_col_list=self.sum_col_list,
            count_col_list=self.count_col_list
        )


@dataclass_json
@dataclass
class Concatenate(Transformation):
    type = "Concatenate"
    df: DataFrame_Extended = None
    cols: List[str] = None
    name: str = None
    delimiter: str = ""

    def transform(self):
        return self.df.concatenate(
            cols=self.cols,
            name=self.name,
            delimiter=self.delimiter)


@dataclass_json
@dataclass
class Split(Transformation):
    type = "Split"
    df: DataFrame_Extended = None
    column: str = None
    newCol: str = None
    delimiter: str = None

    def transform(self):
        return self.df.split(
            column=self.column,
            newCol=self.newCol,
            delimiter=self.delimiter)


@dataclass_json
@dataclass
class AddPerc(Transformation):
    type = "AddPerc"
    df: DataFrame_Extended = None
    column: str = None
    perc_name: str = "perc"

    def transform(self):
        return self.df.add_perc(
            column=self.column,
            perc_name=self.perc_name)


@dataclass_json
@dataclass
class AddDate(Transformation):
    type = "AddDate"
    df: DataFrame_Extended = None
    date: str = None

    def transform(self):
        return self.df.add_date(self.date)


@dataclass_json
@dataclass
class CollectList (Transformation):
    type = "CollectList"
    df: DataFrame_Extended = None
    order_by: List[str] = None
    group_by_list: List[str] = None
    column_list: List[str] = None

    def transform(self):
        return self.df.collect_list(
            order_by=self.order_by,
            group_by_list=self.group_by_list,
            column_list=self.column_list
        )


@dataclass_json
@dataclass
class ListLength(Transformation):
    type = "ListLength"
    df: DataFrame_Extended = None
    column: str = None

    def transform(self):
        return self.df.list_length(
            column=self.column
        )


@dataclass_json
@dataclass
class OneHotEncoder(Transformation):
    type = "OneHotEncoder"
    df: DataFrame_Extended = None
    col: str = None
    newCol: str = None
    vocabSize: int = None

    def transform(self):
        return self.df.one_hot_encoder(
            col=self.col,
            newCol=self.newCol,
            vocabSize=self.vocabSize
        )


@dataclass_json
@dataclass
class ClusterDF(Transformation):
    type = "ClusterDF"
    df: DataFrame_Extended = None
    cluster_col:str = None
    cluster_list:List[str] = None
    groupby_col_list: List[str]=None
    sum_col_list: List[str]=None
    count_col_list: List[str]=None

    def transform(self):
        return self.df.cluster_df(
            cluster_col=self.cluster_col,
            cluster_list=self.cluster_list,
            groupby_col_list=self.groupby_col_list,
            sum_col_list=self.sum_col_list,
            count_col_list=self.count_col_list
        )
