from dataclasses import dataclass
from main.process.transformations.transformation import Transformations
from main.process.transformations.functions.transformations import Transformation_List
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
import logging
from main.process.output.types.output import Output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List
from main.YamlConfig.config import dataclass_json


@dataclass_json
@dataclass
class Parquet(Output):
    type="Parquet"
    partitionCols: List[str] =None

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