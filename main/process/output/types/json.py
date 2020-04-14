from dataclasses import dataclass
from sanformer.main.process.transformations.transformation import Transformations
from sanformer.main.process.transformations.functions.transformations import Transformation_List
from spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended
import logging
from sanformer.main.process.output.output import Output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Json(Output):

    def apply(self, df: DataFrame_Extended):
        name="{}/{}.json".format(self.path,self.name)
        Transformations(
            config=self.transformations,
            transformation_cls=Transformation_List
        ).apply(
            df=df
        ).df.toPandas().to_json(name)
        return logger.info('The output {} has been '
                           'created sucessfully.'.format(self.name))