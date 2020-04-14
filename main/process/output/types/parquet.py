from dataclasses import dataclass
from sanformer.main.process.transformations.transformation import Transformations
from sanformer.main.process.transformations.functions.transformations import Transformation_List
from sanformer.main.process.transformations.transformation import Transformation
from spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
import logging
from sanformer.main.process.output.output import Output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List

@dataclass
class Parquet(Output):
    partitionCols: List[str]

    @classmethod
    def get_from_config(cls, config):
        type = config['type']
        name = config['name']
        path = config['path']
        partitionCols = config['partitionCols']
        transformations = [Transformation.get_from_config(cfg)
                           for cfg in config['transformations']]
        return cls(
            type=type,
            name=name,
            path=path,
            transformations=transformations,
            partitionCols=partitionCols
        )

    def apply(self, df: DataFrame_Extended):
        df=Transformations(
            config=self.transformations,
            transformation_cls=Transformation_List
        ).apply(
            df=df
        )
        name="{}/{}.parquet".format(self.path, self.name)
        df.coalesce(1).write.parquet(name, mode='overwrite')
        return logger.info('The output {} has been '
                           'created sucessfully.'.format(self.name))