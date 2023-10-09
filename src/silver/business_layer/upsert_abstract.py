import json

from abc import ABC, abstractmethod
from logging import Logger
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from src.silver.data_layer.datalayer_abstract import DataLayerAbstract
from pyspark.sql.functions import max


class UpsertAbstract(ABC):

    def __init__(self, logger: Logger, spark : SparkSession,_data_layer):
        self.spark = spark
        self.logger = logger
        self._data_layer = _data_layer


    def run(self):
        try:
            df_output = self.__get_table()
            self.__save_table_to_db(df_output)

            #df_output.writeStream.outputMode("append") \
            #                    .option("checkpointLocation", "checkpoint") \
            #                    .format("console") \
            #                    .start() \
            #                    .awaitTermination()

        except Exception as exception:
            self.logger.error(str(exception))
            raise exception

    def __get_table(self):

        df_offset = self.spark.read.parquet("/workspaces/spark-streaming-sample/datalake/silver/tbl_article_author/pub_year=1988/*.parquet")
        arr_max_offset = df_offset.groupBy("partition").agg(max("offset").alias("max_offset")).collect()
        max_offset = str(arr_max_offset[0].__getitem__('max_offset'))

        df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
                .option("subscribe", "input-topic") \
                .option('failOnDataLoss','false')  \
                .option("enable.auto.commit", "true") \
                .option("kafka.group.id", "test1")  \
                .option("startingOffsets", """{"input-topic":{"0":maxoffset}}""".replace("maxoffset",max_offset)) \
                .load()

               # .option("startingOffsets", "latest") \

        df = self.read_table(df.selectExpr("partition","offset","CAST(key AS STRING)", "CAST(value AS STRING)"))
        return df


    def __save_table_to_db(self, df_pubmed: DataFrame):
        self.data_layer.upsert(df_pubmed)

    @abstractmethod
    def read_table(self, df_pubmed: DataFrame):
        pass

    @property
    @abstractmethod
    def data_layer(self)->DataLayerAbstract:
        pass
