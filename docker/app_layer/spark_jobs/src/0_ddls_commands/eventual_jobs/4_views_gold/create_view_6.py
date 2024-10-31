import os
import logging

from spark_utils import get_spark_session
from wsl_ddl import wslDDL

if __name__ == "__main__":
  silver_table_name = os.getenv("SILVER_TABLE_NAME")
  view_name = os.getenv("GOLD_VIEW_NAME")
  spark_app_name = f"DDL_GOLD_VIEW_{view_name}"
  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info(f"Creating View GOLD {view_name}")

  wsl_ddl = wslDDL(spark, logger)
  wsl_ddl.create_gold_view_6(silver_table_name, view_name)
  logger.info(f"View {view_name} created")