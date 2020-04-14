from dataclasses import dataclass
from sanformer.main.process.input.sources.source import Source
from spark_utils.table_from_database import Table
from spark_utils.dataFrame_extended.dataframe_extended import DataFrame_Extended

@dataclass
class Lake(Source):
    schema: str=None
    name: str=None
    label: str=None

    @classmethod
    def get_from_config(cls, config):

        return cls(
            type=config['type'],
            schema=config['schema'],
            name=config['name'],
            label=config['label'],
            path=config['path'] if 'path' in config.keys() else None
        )

    def apply(self, spark_session, func=None):
        df=Table(
                table_name="{}.{}".format(
                    self.schema,
                    self.name
                ),
                spark_session=spark_session
        ).table
        df_ex=DataFrame_Extended(df=df, spark_session=spark_session)
        return self.transformations(df=df_ex)