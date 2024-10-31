import os
import logging

from spark_utils import get_spark_session
from wsl_ddl import wslDDL

if __name__ == "__main__":

  table_name = os.getenv("TABLE_NAME")
  table_path = os.getenv("TABLE_PATH")
  spark_app_name = f"DDL_BRONZE_TABLE_{table_name}"

  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info(f"Creating BRONZE table {table_name}")


  wsl_ddl = wslDDL(spark, logger)
  wsl_ddl.create_bronze_table(table_name, table_path)
  logger.info(f"Table {table_name} created on path {table_path}!")