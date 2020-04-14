from dataclasses import dataclass
from main.process.transformations.transformation import Transformations
from main.process.transformations.functions.transformations import Transformation_List
from main.process.transformations.transformation import Transformation
from spark_utils import DataFrame_Extended
import logging
from main.process.output.output import Output
from src.statistics.main.visualizations.santander_plots import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Plot(Output):
    plot_type: str
    attributes: dict

    @classmethod
    def get_from_config(cls, config):
        type = config['type']
        name = config['name']
        path = config['path']
        plot_type = config['plot_type']
        attributes = config['attributes']
        transformations = [Transformation.get_from_config(cfg)
                           for cfg in config['transformations']]
        return cls(
            type=type,
            name=name,
            path=path,
            transformations=transformations,
            plot_type=plot_type,
            attributes=attributes
        )

    def apply(self, df: DataFrame_Extended):
        df=Transformations(
            config=self.transformations,
            transformation_cls=Transformation_List
        ).apply(
            df=df
        )
        if self.plot_type=="pie-chart": PieChart(df=df, path=self.path,name=self.name,**self.attributes).run()
        if self.plot_type == "hist-chart": HistChart(df=df, path=self.path,name=self.name, **self.attributes).run()
        return logger.info('The output {} has been '
                           'created sucessfully.'.format(self.name))