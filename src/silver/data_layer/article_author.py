from logging import lastResort
from src.silver.data_layer.datalayer_abstract import DataLayerAbstract
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf

class ArticleAuthor(DataLayerAbstract):

    table_name = 'tbl_article_author'

    column_list = [
        "key as file_name",
        "xpath_string(value, '/root/PubmedArticle/MedlineCitation/PMID/text()') as pmid",
        "xpath_string(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/LastName/text()') as last_name",
        "xpath_string(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/ForeName/text()') as fore_name",
        "xpath_string(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/Initials/text()') as Initials"
        ]


    def get_latest_records(self, df: DataFrame) -> DataFrame:
        latest_records_df = df
        return latest_records_df
