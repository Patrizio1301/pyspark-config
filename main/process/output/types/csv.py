from dataclasses import dataclass
from main.process.transformations.transformation import Transformations
from main.process.transformations.functions.transformations import Transformation_List
from main.spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
import logging
from main.process.output.output import Output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Csv(Output):

    def apply(self, df: DataFrame_Extended):
        df=Transformations(
            config=self.transformations,
            transformation_cls=Transformation_List
        ).apply(
            df=df
        )
        name="{}/{}.csv".format(self.path, self.name)
        df.repartition(1).write.csv(name, header=True, mode="overwrite")
        return logger.info('The output {} has been '
                           'created sucessfully.'.format(self.name))