import os
import logging
from datetime import datetime as dt

from pyspark.sql.functions import lit
from spark_utils import get_spark_session


class ActorStagingToBronze:

  def __init__(self, logger, spark):
    self.spark = spark
    self.logger = logger
    self.path_staging = None
    self.df_staging = None
    self.df_transformed = None


  def create_bronze_table(self, table_name, table_path):
    self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[0]}")
    self.spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
      value STRING,
      date_ref STRING)
    USING DELTA
    LOCATION '{table_path}'
    PARTITIONED BY (date_ref)""")
    return self

  def config_path_staging_files(self, prefix, execution_date):
    partitioned_path = dt.strftime(execution_date, 'year=%Y/month=%m/day=%d/hour=%H')
    self.path_staging = f"{prefix}/{partitioned_path}"
    self.logger.info(f"Path Staging: {self.path_staging}")
    return self
  
      
  def read_from_staging(self):
    assert self.path_staging is not None, "Path for staging dir must be configured"
    self.df_staging = (
      self.spark.read.format("text")
        .option("compression", "gzip")
        .load(f"{self.path_staging}/*"))
    return self

  
  def write_to_bronze(self, table_name, partition_ohour):
    assert self.df_staging is not None, "You must read the files from staging"
    _ = (
      self.df_transformed
        .withColumn("date_ref", lit(partition_ohour))
        .write
        .mode("overwrite")
        .partitionBy("date_ref") \
        .option("replaceWhere", f"date_ref = '{partition_ohour}'")
        .saveAsTable(table_name))
    return True



if __name__ == "__main__":
  
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.addHandler(logging.StreamHandler())
  EXECUTION_DATE = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
  
  PREFIX = "/mnt/wsl_analytics/staging"
  BRONZE_TABLE_NAME = "bronze.logs"
  BRONZE_TABLE_PATH = "/mnt/wsl_analytics/bronze"

  EXECUTION_DATE = dt.strptime(EXECUTION_DATE, '%Y-%m-%d %H:%M:%S%z')
  partition_ohour = dt.strftime(EXECUTION_DATE, "%Y-%m-%d-%H")

  spark = get_spark_session(spark_app_name)
  spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE_NAME} PURGE")


  status = (
    ActorStagingToBronze(spark, logger)
      .create_bronze_table(BRONZE_TABLE_NAME, BRONZE_TABLE_PATH)
      .config_path_staging_files(PREFIX, EXECUTION_DATE)
      .read_from_staging()
      .write_to_bronze(BRONZE_TABLE_NAME, partition_ohour)
  )
