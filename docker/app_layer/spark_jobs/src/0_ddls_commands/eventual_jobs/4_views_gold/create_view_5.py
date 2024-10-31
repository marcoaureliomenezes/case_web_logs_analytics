import os
import logging

from spark_utils import get_spark_session
from wsl_ddl import wslDDL

if __name__ == "__main__":
  silver_table_name = os.getenv("SILVER_TABLE_NAME")

  spark = get_spark_session("CREATE_VIEWS_GOLD")
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.info(f"Creating Views GOLD")

  wsl_ddl = wslDDL(spark, logger)
  wsl_ddl.create_gold_view_5_1(silver_table_name, "nessie.gold.total_traffic")
  wsl_ddl.create_gold_view_5_2(silver_table_name, "nessie.gold.biggest_payload")
  wsl_ddl.create_gold_view_5_3(silver_table_name, "nessie.gold.smallest_payload")
  wsl_ddl.create_gold_view_5_4(silver_table_name, "nessie.gold.average_payload")
  logger.info(f"Views created")