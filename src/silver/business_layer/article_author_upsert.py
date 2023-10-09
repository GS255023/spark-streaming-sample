from src.silver.business_layer.upsert_abstract import UpsertAbstract
from pyspark.sql import DataFrame
from  pyspark.sql.functions import col,explode,expr,arrays_zip,lit
from src.common.utility import generate_hash

class ArticleAuthorUpsert(UpsertAbstract):

    @property
    def data_layer(self):
        return self._data_layer

    def read_table(self,df_articleauthor: DataFrame):

        df_articleauthor = df_articleauthor \
                            .withColumn("pub_year",lit("1988"))  \
                            .withColumn("partition",expr("partition"))  \
                            .withColumn("offset",expr("offset"))  \
                            .withColumn("key",expr("key"))  \
                            .withColumn("pmid", expr("xpath_string(value, '/root/PubmedArticle/MedlineCitation/PMID/text()') as pmid"))  \
                            .withColumn("authors",explode(arrays_zip(
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/LastName/text()') as last_name"),
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/ForeName/text()') as fore_name"),
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/Initials/text()') as Initials")
                                    ).cast("array<struct<last_name:string,fore_name:string,Initials:string>>"))) \
                            .select("pub_year","partition","offset","key", "pmid", "authors.*")
        df_articleauthor = df_articleauthor.withColumn('article_author_hash',generate_hash(df_articleauthor.pmid))
        return df_articleauthor