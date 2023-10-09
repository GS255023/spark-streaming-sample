from src.common.spark_init import SparkInit
from src.silver.business_layer.article_author_upsert import ArticleAuthorUpsert
from src.silver.data_layer.article_author import ArticleAuthor
from logging import Logger
from pyspark.sql.session import SparkSession


class Runner():
    '''
    Initalize and medline citation upsert here
    '''

    def __init__(self, spark: SparkSession, logger: Logger):
        self.spark = spark
        self.logger = logger


    def run(self):
        #Parse Author Information from XML FILE
        articleauthor = ArticleAuthor(self.logger, self.spark)
        articleauthor_upsert = ArticleAuthorUpsert(self.logger, self.spark, articleauthor)
        articleauthor_upsert.run()


if __name__ == '__main__':
    try:
        spark_init = SparkInit()
        runner = Runner(spark_init.spark, spark_init.logger)
        runner.run()
    except Exception as exception:
        spark_init.logger.error(str(exception))
        raise exception
