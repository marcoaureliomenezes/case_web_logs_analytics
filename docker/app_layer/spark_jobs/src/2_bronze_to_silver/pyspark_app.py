import os
import logging

from datetime import datetime as dt

from pyspark.sql.functions import lit, col, split, regexp_replace, date_format, to_timestamp, from_utc_timestamp, to_utc_timestamp, concat
from spark_utils import get_spark_session


class ActorBronzeToSilver:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger
    self.path_stagging = None
    self.df_staging = None
    self.df_transformed = None


  def read_from_bronze(self, bronze_table_name, ohour):
    self.df_bronze =  self.spark.table(bronze_table_name).filter(col("date_ref") == ohour)
    return self

  def parse_to_silver_schema(self):
    assert self.df_bronze is not None, "You must read bronze table first using 'read_from_bronze' method"
    df_parsed_schema = (
      self.df_bronze.
       withColumn('log_clean', regexp_replace('value', '[\\[\\]""]', ''))
      .withColumn('log_clean', split(col('log_clean'), '  '))
    )

    df_parsed_schema.show(20, False)
    self.df_parsed_schema = df_parsed_schema.select(
        col('log_clean').getItem(0).alias('ip_address'),
        col('log_clean').getItem(2).alias('user'),
        col('log_clean').getItem(3).alias('datetime'),
        col('log_clean').getItem(4).alias('timezone'),
        col('log_clean').getItem(5).alias('http_method'),
        col('log_clean').getItem(6).alias('http_route'),
        col('log_clean').getItem(7).alias('http_protocol'),
        col('log_clean').getItem(8).cast('int').alias('http_status'),
        col('log_clean').getItem(9).cast('int').alias('payload_size')
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


if __name__ == "__main__":
  
  APP_NAME = "BRONZE_TO_SILVER"
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.addHandler(logging.StreamHandler())

  EXECUTION_DATE = os.getenv("EXECUTION_DATE")
  BRONZE_TABLE_NAME = os.getenv("BRONZE_TABLE_NAME")
  SILVER_TABLE_NAME = os.getenv("SILVER_TABLE_NAME")
  SILVER_TABLE_PATH = os.getenv("SILVER_TABLE_PATH")

  EXECUTION_DATE = dt.strptime(EXECUTION_DATE, '%Y-%m-%d %H:%M:%S%z')
  partition_ohour = dt.strftime(EXECUTION_DATE, "%Y-%m-%d-%H")

  spark = get_spark_session(APP_NAME)

  status = (
    ActorBronzeToSilver(spark, logger)
      .read_from_bronze(BRONZE_TABLE_NAME, partition_ohour)
      .parse_to_silver_schema()
      .cast_and_transform_to_silver()
      .write_to_silver(SILVER_TABLE_NAME)
  )
