from typing import List
from main.process.dfo import DFO

class InputReader(object):
    def __init__(self, sources: List[DFO]):
        self.sources=sources

    def find(self, label):
        for source in self.sources:
            if source.label==label:
                return source
        raise Exception("The input source {} is not define.".format(label))

class DataFrame_Creation(object):
    def __init__(self, sources):
        self.sources=InputReader(sources)

    def identity(self, label:str):
        return self.sources.find(label)

    def join(self, left: str, right: str,
             left_on: List[str], right_on: List[str], how:str):
        left_df=self.sources.find(label=left)
        right_df = self.sources.find(label=right)
        return left_df.df.join_customized(
            other=right_df.df.df,
            left_on=left_on,
            right_on=right_on,
            how=how
        )

class DataFrame_Input_Transformations(object):
    def __init__(self, df, sources):
        self.sources=InputReader(sources)
        self.df=df

    def join(self, right: str,
             left_on: List[str], right_on: List[str], how: str):
        left_df = self.df
        right_df = self.sources.find(label=right)
        return left_df.join_customized(
            other=right_df.df.df,
            left_on=left_on,
            right_on=right_on,
            how=how
        )
