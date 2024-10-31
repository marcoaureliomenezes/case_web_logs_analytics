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


  def create_bronze_table(self, table_name, table_path):
    self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    self.spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        value STRING    NOT NULL COMMENT 'Raw log line',
        date_ref STRING NOT NULL COMMENT 'Date reference'
      )
      USING iceberg
      LOCATION '{table_path}'
      PARTITIONED BY (date_ref)
      TBLPROPERTIES ('gc.enabled' = 'true')""")
    self.spark.table(table_name).printSchema()
    self.logger.info(f"Table {table_name} created")
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
      self.df_staging
        .withColumn("date_ref", lit(partition_ohour))
        .write
        .mode("overwrite")
        .partitionBy("date_ref") \
        .option("replaceWhere", f"date_ref = '{partition_ohour}'")
        .saveAsTable(table_name))
    return True


if __name__ == "__main__":
  
  APP_NAME = "STAGING_TO_BRONZE"
  logger = logging.getLogger(APP_NAME)
  logger.setLevel(logging.INFO)
  logger.addHandler(logging.StreamHandler())

  EXECUTION_DATE = os.getenv("EXECUTION_DATE")
  STAGING_PATH = os.getenv("STAGING_PATH")
  BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH")
  BRONZE_TABLE_NAME = os.getenv("BRONZE_TABLE_NAME")


  EXECUTION_DATE = dt.strptime(EXECUTION_DATE, '%Y-%m-%d %H:%M:%S%z')
  partition_ohour = dt.strftime(EXECUTION_DATE, "%Y-%m-%d-%H")

  spark = get_spark_session(APP_NAME)

  status = (
    ActorStagingToBronze(spark, logger)
      .create_bronze_table(BRONZE_TABLE_NAME, BRONZE_TABLE_PATH)
      .config_path_staging_files(STAGING_PATH, EXECUTION_DATE)
      .read_from_staging()
      .write_to_bronze(BRONZE_TABLE_NAME, partition_ohour)
  )
