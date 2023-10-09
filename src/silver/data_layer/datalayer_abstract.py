import os
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession,DataFrame
from logging import Logger

class DataLayerAbstract(ABC):

    '''
    Apache Hudi implementation for all tables
    '''
    def __init__(self, logger: Logger, spark: SparkSession):
        self.logger = logger
        self.spark = spark
        self.write_base_path = f'/workspaces/spark-streaming-sample/datalake/silver/{str(self.table_name)}'
        self.checkpointPath = f'/workspaces/spark-streaming-sample/checkpoint/silver/{str(self.table_name)}/'


    def upsert(self, pubmed_df : DataFrame):
        try:
            writer = pubmed_df.writeStream.format('parquet') \
                                        .option('checkpointLocation',self.checkpointPath) \
                                        .partitionBy("pub_year" ) \
                                        .start(self.write_base_path) \
                                        .awaitTermination()

        except Exception as ex:
            self.logger.error(f'Error : {str(ex)}')
            raise ex

    @abstractmethod
    def get_latest_records(self, df: DataFrame) -> DataFrame:
         pass

    @property
    @abstractmethod
    def column_list(self):
        pass

    @property
    @abstractmethod
    def table_name(self):
        pass

