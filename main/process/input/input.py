from main.process.transformations.functions.input_transformations import DataFrame_Creation, DataFrame_Input_Transformations
from dataclasses import dataclass
from main.process.transformations.transformation import Transformation
from typing import List
import logging
from process.dfo import DFO
from main.process.input.sources.source import Source
from main.process.input.sources.constructor import Constructor
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Input(object):
    sources: List[Source]
    transformations: List[Transformation] = None

    @classmethod
    def get_from_config(cls, config):
        sources=[Constructor.get_from_config(source)
                 for source in config['sources']]
        try:
            trans=[Transformation.get_from_config(trans)
                   for trans in config['transformations']]
        except KeyError:
            trans= None
        except Exception as error:
            raise Exception(error)

        return cls(
            sources=sources,
            transformations=trans
        )

    def table_dict(self, spark_session):
        """
        List of all input dataFrames alias DFOs.

        :return: List of DFOs.

        """
        return [DFO(
            df=source.apply(spark_session),
            label=source.label
        ) for source in self.sources]

    def apply(self, spark_session):
        """
        Created the input dataFrame.

        """
        sources_dict=self.table_dict(spark_session)
        if not self.transformations:
            if len(sources_dict)==0:
                raise Exception("Missing input.")
            elif len(sources_dict)==1:
                return next(iter(sources_dict)).df
            else:
                raise Exception("Multiple sources to choose from. "
                                "Specify transformations in order "
                                "to handle all sources or quit sources.")
        else:
            if len(self.transformations)== 1:
                return self.initial_transformation(
                    sources=sources_dict,
                    trans=self.transformations[0]
                )
            else:
                transformation_config=self.transformations[1:]

                df=self.initial_transformation(
                    sources=sources_dict,
                    trans=self.transformations[0]
                )
                for trans in transformation_config:
                    df = self.apply_transformation(
                        df=df,
                        trans=trans,
                        sources=sources_dict
                    )
                return df


    def initial_transformation(self, sources: List[DFO], trans: Transformation):
        input = DataFrame_Creation(sources=sources)
        return getattr(input, trans.type)(**trans.attributes)

    def apply_transformation(self, df, trans: Transformation, sources: List[DFO]):
        input = DataFrame_Input_Transformations(df=df, sources=sources)
        return getattr(input, trans.type)(**trans.attributes)

