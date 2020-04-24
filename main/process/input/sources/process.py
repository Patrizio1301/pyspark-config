from dataclasses import dataclass
from typing import List

from main.process.input.sources.source import Source
from main.process.dfo import DFO

@dataclass
class Process(Source):

    @classmethod
    def get_from_config(cls, config):
        return cls(
            type=config['type'],
            label=config['label'],
            path=config['path']
        )

    def apply(self, spark_session, dfs:List[DFO]=None):
        for df in dfs:
            if df.label==self.label:
                return self.transformations(df=df.df)
        raise ValueError("Input source {} cannot be found".format(self.label))