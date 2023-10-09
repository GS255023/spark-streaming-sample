import pyspark.sql.functions as sf
from pyspark.sql import Column

def generate_hash(*column: Column):
    return sf.sha2(
        sf.lower(sf.concat_ws('', *column)),
        256
    )