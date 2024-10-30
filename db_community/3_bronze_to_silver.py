# Databricks notebook source
import os
import logging
from datetime import datetime as dt
from pyspark.sql.functions import lit, col, split, regexp_replace, date_format, to_timestamp, from_utc_timestamp, to_utc_timestamp, concat

class ActorBronzeToSilver:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger
    self.path_stagging = None
    self.df_staging = None
    self.df_transformed = None

  def __drop_silver_table_when_needed(self, table_name, table_path):
    self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

  def create_silver_table(self, table_name, table_path):
    self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[0]}")
    self.__drop_silver_table_when_needed(table_name, table_path)
    self.spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
      ip_address STRING,
      user STRING,
      response_utc_timestamp TIMESTAMP,
      http_method STRING,
      http_route STRING,
      http_protocol STRING,
      http_status INT,
      payload_size INT,
      date_ref STRING)
    USING DELTA
    LOCATION '{table_path}'
    PARTITIONED BY (date_ref)""")
    return self


  def read_from_bronze(self, bronze_table_name, ohour):
    self.df_bronze =  self.spark.table(bronze_table_name).filter(col("date_ref") == ohour)
    return self

  def parse_to_silver_schema(self):
    assert self.df_bronze is not None, "You must read bronze table first using 'read_from_bronze' method"
    df_clean = self.df_bronze.withColumn('log_clean', regexp_replace('value', '[\\[\\]""]', ''))
    df_split = df_clean.withColumn('parts', split(col('log_clean'), ' '))
    self.df_parsed_schema = df_split.select(
        col('parts').getItem(0).alias('ip_address'),
        col('parts').getItem(2).alias('user'),
        col('parts').getItem(3).alias('datetime'),
        col('parts').getItem(4).alias('timezone'),
        col('parts').getItem(5).alias('http_method'),
        col('parts').getItem(6).alias('http_route'),
        col('parts').getItem(7).alias('http_protocol'),
        col('parts').getItem(8).cast('int').alias('http_status'),
        col('parts').getItem(9).cast('int').alias('payload_size')
    )
    return self

  def cast_and_transform_to_silver(self):
    assert self.df_parsed_schema is not None, "The schema must be parsed before with 'parse_to_silver_schema' method"
    self.df_transformed = (
      self.df_parsed_schema
        .withColumn("response_timestamp", to_timestamp(col("datetime"), "dd/MMM/yyyy:HH:mm:ss")).drop("datetime")
        .withColumn('response_utc_timestamp', from_utc_timestamp(to_utc_timestamp(col('response_timestamp'), 'GMT'), concat(lit("GMT"), col('timezone'))))
        .drop("response_timestamp", "timezone")
        .withColumn("http_status", col("http_status").cast("int"))
        .withColumn("payload_size", col("payload_size").cast("int"))
        .withColumn("date_ref", date_format(col("response_utc_timestamp"), "yyyy-MM-dd"))
    )
    return self
  
  def write_to_silver(self, table_name):
    assert self.df_transformed is not None, "The dataframe must be casted and transformed before with 'cast_and_transform_to_silver' method"
    final_cols = ["ip_address", "user", "response_utc_timestamp", "http_method", "http_route", "http_protocol", "http_status", "payload_size", "date_ref"]
    df_to_write = self.df_transformed.select(*final_cols)
    df_to_write.show()
    df_to_write.printSchema()
    _ = (
      df_to_write.write
      .mode("overwrite")
      .partitionBy("date_ref")
      .saveAsTable(table_name)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entrypoint

# COMMAND ----------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
EXECUTION_DATE = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")

BRONZE_TABLE_NAME = "bronze.logs"
SILVER_TABLE_NAME = "silver.logs"
SILVER_TABLE_PATH = "/mnt/wsl_analytics/silver"

EXECUTION_DATE = dt.strptime(EXECUTION_DATE, '%Y-%m-%d %H:%M:%S%z')
partition_ohour = dt.strftime(EXECUTION_DATE, "%Y-%m-%d-%H")

status = (
  ActorBronzeToSilver(logger)
    .create_silver_table(SILVER_TABLE_NAME, SILVER_TABLE_PATH)
    .read_from_bronze(BRONZE_TABLE_NAME, partition_ohour)
    .parse_to_silver_schema()
    .cast_and_transform_to_silver()
    .write_to_silver(SILVER_TABLE_NAME)
)
