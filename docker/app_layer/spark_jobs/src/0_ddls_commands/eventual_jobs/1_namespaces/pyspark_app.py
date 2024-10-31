import os
import logging

from spark_utils import get_spark_session
from wsl_ddl import wslDDL

if __name__ == "__main__":

  namespace = os.getenv("NAMESPACE_NAME")
  spark_app_name = f"DDL_CREATE_NAMESPACE_{namespace.replace('.', '_').upper()}"
  spark = get_spark_session(spark_app_name)
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)

  wsl_ddl = wslDDL(spark, logger)
  wsl_ddl.create_namespace(namespace)
  logger.info(f"Namespace {namespace} created")