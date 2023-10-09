import logging
from pyspark.sql import SparkSession

class SparkInit:
    '''
    Spark initialization and Hudi write options
    '''

    def __init__(self):
        self.spark = SparkSession.builder\
                        .appName('Streaming_Excercise') \
                        .getOrCreate()
        self.logger = self.set_up_logger(self.spark)

    # reuse Spark logger and display logs with WARN level so that they will show up
    def set_up_logger(self, spark):
        logging.basicConfig(level=logging.WARN,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        spark_logger = spark._jvm.org.apache.log4j.Logger
        return spark_logger.getLogger(__name__)