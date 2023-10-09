# KAFKA Structured Streaming

This is Spark Structured Streaming (Python or Scala) application in working condition.

## Data Flow

# Real Time processing
    Kafka -> Silver -> Gold

# Batch Processing
    Landing Area -> Bronze -> Silver -> Gold

# Source Code Directory Structure

- Source code is divided into 3 directories.
  i)Bronze ii)Silver iii)Gold

  - Silver:

    - job_interface: Main runner aplication or entry point of spark application.

    - data_layer: This layer deals with data attributes of the object like column_list,table_name which needs to extracted,processed and ingested into the target table. This layer also helps to intreact with underlying storage like storing data to target table or reading from target table. Common functionality is abstracted into Abstract class to bring uniformity and avoiding code repitition for different data objects.

    - business_layer: This layer deals with business logic required to process the data before and it is stored in Silver datalake like flattening of xml structure , extracting of business primary key like PMID etc. Again common functionality is abstracted into Abstract class to bring uniformity and avoiding code repitition for different business objects.


## Steps

## Publishing Data

- kcat -P -b localhost:9092 -t input-topic -p -1 -k file1 pubmed21n0000.xml

  We can use DirectoryWatcher or any other technology to run this command in real time. This command will publish data to kakfka topic "input-topic" with key "file2" + Random partition (-1) and value is the contents of xml file i.e pubmed21n0000.xml. This sample xml file was downloaded from PubMed® which comprises more than 36 million citations for biomedical literature.

  Ideally Databricks recommends using Auto Loader for streaming ingestion from cloud object storage. Auto Loader supports most file formats supported by Structured Streaming.

## Consuming Data

- Once data is published to kafka, kafka stream will start reading data from kafka in real time

      df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
                .option("subscribe", "input-topic") \
                .option('failOnDataLoss','false') \
                .option("kafka.group.id", "test1") \
                .option("startingOffsets", """{"input-topic":{"0":maxoffset}}""".replace("maxoffset",max_offset)) \
                .load()

- Now it is mentioned that when starting the stream we should read offsets from "maxoffset" attribute. Design architecture consideration is to mantain the kafka lineage alongwith the data in the target sink table (parquet) in Silver layer.

              +---------+------+---------+----+---------+---------+--------+--------------------+
              |partition|offset|      key|pmid|last_name|fore_name|Initials| article_author_hash|
              +---------+------+---------+----+---------+---------+--------+--------------------+
              |        0|    10|file2.csv|  72|   Elgart|      E S|      ES|8722616204217eddb...|
              |        0|    10|file2.csv|  72| Gusovsky|        T|       T|8722616204217eddb...|
              |        0|    10|file2.csv|  72|Rosenberg|      M D|      MD|8722616204217eddb...|
              |        0|    10|file2.csv|  72|Hallworth|      G W|      GW|8722616204217eddb...|
              |        0|    10|file2.csv|  72| Hamilton|      R R|      RR|8722616204217eddb...|

        df_offset = self.spark.read.parquet("/workspaces/spark-streaming-sample/datalake/silver/tbl_article_author/*.parquet")
        arr_max_offset = df_offset.groupBy("partition").agg(max("offset").alias("max_offset")).collect()
        max_offset = str(arr_max_offset[0].__getitem__('max_offset'))

## Transforming Data
- Once data is read for every message in xml format data is flattened using spark transformation.

      # Flattern the xml strucuture

            df_articleauthor = df_articleauthor \
                            .withColumn("partition",expr("partition"))  \
                            .withColumn("offset",expr("offset"))  \
                            .withColumn("key",expr("key"))  \
                            .withColumn("pmid", expr("xpath_string(value, '/root/PubmedArticle/MedlineCitation/PMID/text()') as pmid"))  \
                            .withColumn("authors",explode(arrays_zip(
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/LastName/text()') as last_name"),
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/ForeName/text()') as fore_name"),
                                    expr("xpath(value, '/root/PubmedArticle/MedlineCitation/Article/AuthorList/Author/Initials/text()') as Initials")
                                    ).cast("array<struct<last_name:string,fore_name:string,Initials:string>>"))) \
                            .select("partition","offset","key", "pmid", "authors.*")



## Upsert Operation
- If unserlying transaction supports ACID transaction or allow Upsert opeation like (Apache HUDI) we can use the hashing function to generetate the recordkey.field.

            df_articleauthor.withColumn('article_author_hash',generate_hash(df_articleauthor.pmid))


## Writing Stream Data to Datalale

- Data is streamed and stored in Datalake in parquet format and also checkpoint  helps to build fault-tolerant and resilient Spark applications.
- Ideally as we are reading data from kafka directly we should be streamed transformed data into HUDI table or Delta lake Silver tables to avoid hoping from Bronze to Silver layer step.

                      writer = pubmed_df.writeStream.format('parquet') \
                                        .option('checkpointLocation',self.checkpointPath) \
                                        .partitionBy("pub_year" ) \
                                        .start(self.write_base_path) \
                                        .awaitTermination()
- Partitioned Datalake
  Datalake is partitioned on pub_year column like
      /datalake/silver/tbl_article_author/pub_year=1988/


## Installation

Prerequisites:
- Kakfa: https://kafka.apache.org/downloads.html
- Zookeper: https://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html
- Docker Image: https://hub.docker.com/r/jupyter/all-spark-notebook
- Docker VSCODE extension: https://code.visualstudio.com/docs/containers/overview
- kcat: brew install kcat

## Packages

- packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

## Scripts

- spark-submit.sh src/silver/job_interface/upsert_runner.py 128M