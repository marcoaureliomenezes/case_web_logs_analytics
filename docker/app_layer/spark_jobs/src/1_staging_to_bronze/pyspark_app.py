import os
import logging
from datetime import datetime as dt

from pyspark.sql.functions import lit
from spark_utils import get_spark_session


class ActorStagingToBronze:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger

    self.path_staging = None
    self.df_staging = None
    self.df_transformed = None


  def config_path_staging_files(self, prefix, server_name, execution_date):
    partitioned_path = dt.strftime(execution_date, 'year=%Y/month=%m/day=%d')
    self.path_staging = f"{prefix}/{server_name}/{partitioned_path}"
    self.logger.info(f"Path Staging: {self.path_staging}")
    return self
  
      
  def read_from_staging(self):
    assert self.path_staging is not None, "Path for staging dir must be configured"
    self.df_staging = (
      self.spark.read.format("text")
        .option("compression", "gzip")
        .load(f"{self.path_staging}/*"))
    return self

  
  def write_to_bronze(self, table_name, partition_ohour, server_name):
    assert self.df_staging is not None, "You must read the files from staging"
    df_to_write = (
      self.df_staging
        .withColumn("date_ref", lit(partition_ohour))
        .withColumn("server_name", lit(server_name))
    )
    df_to_write.show(10)
    self.logger.info(f"Writing to bronze table {table_name}")
    # write to iceberg table overwriting the partition date_ref and server_name
    df_to_write.writeTo(table_name).overwritePartitions()

    return True


if __name__ == "__main__":
  
  APP_NAME = "STAGING_TO_BRONZE"
  logger = logging.getLogger(APP_NAME)
  logger.setLevel(logging.INFO)
  logger.addHandler(logging.StreamHandler())

  EXECUTION_DATE = os.getenv("EXECUTION_DATE")
  STAGING_PATH = os.getenv("STAGING_PATH")
  SERVER_NAME = os.getenv("SERVER_NAME")
  BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH")
  BRONZE_TABLE_NAME = os.getenv("BRONZE_TABLE_NAME")


  EXECUTION_DATE = dt.strptime(EXECUTION_DATE, '%Y-%m-%d %H:%M:%S%z')
  partition_ohour = dt.strftime(EXECUTION_DATE, "%Y-%m-%d")

  spark = get_spark_session(APP_NAME)

  status = (
    ActorStagingToBronze(spark, logger)
      .config_path_staging_files(STAGING_PATH, SERVER_NAME, EXECUTION_DATE)
      .read_from_staging()
      .write_to_bronze(BRONZE_TABLE_NAME, partition_ohour, SERVER_NAME)
  )
