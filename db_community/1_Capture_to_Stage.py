# Databricks notebook source
# MAGIC %pip install rand-engine==0.2.0 faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Classe WSLBatchGenerator
# MAGIC  

# COMMAND ----------

import time
import csv
import faker
import os
import logging

from datetime import datetime as dt
from rand_engine.core.distinct_core import DistinctCore
from rand_engine.core.numeric_core import NumericCore
from rand_engine.core.datetime_core import DatetimeCore
from rand_engine.core.distinct_utils import DistinctUtils
from rand_engine.main.dataframe_builder import BulkRandEngine
from datetime import datetime as dt, timedelta


class WSLBatchGenerator:

  @staticmethod
  def metadata_case_web_log_server(formato, dt_start, dt_end):
    fake = faker.Faker(locale="pt_BR")
    metadata = {
      "ip_address":dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=[fake.ipv4_public() for i in range(1000)])),
      "identificador": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "user": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "user_named": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=[fake.first_name().lower().replace(" ", "_") for i in range(1000)])),
      "datetime": dict(
        method=DatetimeCore.gen_datetimes, 
        parms=dict(start=dt_start, end=dt_end, format_in=formato, format_out="%d/%b/%Y:%H:%M:%S")
      ),
      "http_version": dict(
        method=DistinctCore.gen_distincts_typed,
        parms=dict(distinct=DistinctUtils.handle_distincts_lvl_1({"HTTP/1.1": 7, "HTTP/1.0": 3}, 1))
      ),
      "campos_correlacionados_proporcionais": dict(
        method=       DistinctCore.gen_distincts_typed,
        splitable=    True,
        cols=         ["http_request", "http_status"],
        sep=          ";",
        parms=        dict(distinct=DistinctUtils.handle_distincts_lvl_3({
                          "GET /home": [("200", 7),("400", 2), ("500", 1)],
                          "GET /login": [("200", 5),("400", 3), ("500", 1)],
                          "POST /login": [("201", 4),("404", 2), ("500", 1)],
                          "GET /logout": [("200", 3),("400", 1), ("400", 1)],
                          "POST /signin": [("201", 4),("404", 2), ("500", 1)],
                          "GET /balance": [("200", 3),("400", 1), ("500", 1)],
                          "POST /loans/make_loan": [("200", 3),("400", 1), ("500", 1)],
                          "GET /credit/statement.pdf": [("200", 3),("400", 1), ("500", 1)],
                          "GET /account/statement.pdf": [("200", 3),("400", 1), ("500", 1)],
                          
          }))
      ),
      "object_size": dict(method=NumericCore.gen_ints, parms=dict(min=0, max=10000)),
    }
    return metadata


  @staticmethod
  def web_server_log_transformer(df):
    authenticated_endpoints = ['GET /home', 'GET /logout', 'GET /balance', 'GET /credit/statement.pdf', 'GET /account/statement.pdf']
    associate_user_with_http_request = lambda x: x['user_named'] if x['http_request'] in authenticated_endpoints else '-'
    df['user'] = df.apply(associate_user_with_http_request, axis=1)
    df = df['ip_address'] + ' ' + df['identificador'] + ' ' + df['user'] + ' [' + df['datetime'] + ' -0300] "' + \
                        df['http_request'] + ' ' + df['http_version'] + '" ' + df['http_status'] + ' ' + df['object_size'].astype(str)
    return df

  @staticmethod
  def handle_timestamp(dt_end, hours_back):
    dt_start = dt_end - timedelta(hours=hours_back)
    input_format = "%Y-%m-%d %H:%M:%S"
    return input_format, dt_start.strftime(input_format), dt_end.strftime(input_format)


  @staticmethod
  def generate_file_name(dt_start, dt_end):
    dt_start, dt_end = [dt.strptime(i, "%Y-%m-%d %H:%M:%S") for i in [dt_start, dt_end]]
    timestamp_start, timestamp_end = [int(dt.timestamp(i)) for i in [dt_start, dt_end]]
    return f"web_server_log_{timestamp_start}_{timestamp_end}.log"


  @staticmethod
  def generate_wsl_df(spark, metadata, transformer, size=1000):
    bulk_engine = BulkRandEngine()
    df = bulk_engine.create_pandas_df(metadata=metadata, size=size)
    df = df.sort_values(by='datetime')
    df = transformer(df)
    df_spark = spark.createDataFrame(df.to_frame())
    del df
    return df_spark
  


# COMMAND ----------

class EnvConfigurator:

    @staticmethod
    def create_enviromnent(dbutils):
        dbutils.fs.mkdirs("mnt/wsl_analytics/staging")
        dbutils.fs.mkdirs("mnt/wsl_analytics/bronze")
        dbutils.fs.mkdirs("mnt/wsl_analytics/silver")
        dbutils.fs.mkdirs("mnt/wsl_analytics/gold")
        display(dbutils.fs.ls("/mnt/wsl_analytics"))


class WSLIngestor:
  
    def __init__(self, logger):
        self.logger = logger

    def config_file_prefix(self, lake_path, execution_date):
        execution_date = dt.strptime(execution_date, '%Y-%m-%d %H:%M:%S%z')
        partitioned_path = dt.strftime(execution_date, 'year=%Y/month=%m/day=%d/hour=%H')
        path = f"{lake_path}/{partitioned_path}"
        self.logger.info(f"Path Staging: {path}")
        return path

    def write_to_staging(self, df_spark, path):
        df_spark.write \
            .mode("overwrite") \
            .text(path, compression="gzip")
        self.logger.info(f"Data written to {path}")
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entrypoint

# COMMAND ----------


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
EXECUTION_DATE = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
BUCKET = os.getenv("BUCKET", "warehouse")
LAYER = "mnt/wsl_analytics/staging"

logger.info(f"Execution Date: {EXECUTION_DATE}")
logger.info(f"Bucket: {BUCKET}")
logger.info(f"Layer: {LAYER}")

dt_execution_date = dt.strptime(EXECUTION_DATE, "%Y-%m-%d %H:%M:%S%z")
hours_back = 6

formato, dt_start, dt_end = WSLBatchGenerator.handle_timestamp(dt_execution_date, hours_back)
metadata = WSLBatchGenerator.metadata_case_web_log_server(formato, dt_start, dt_end)
transformer = WSLBatchGenerator.web_server_log_transformer

wsl_ingestor = WSLIngestor(logger)
path = wsl_ingestor.config_file_prefix(LAYER, EXECUTION_DATE)
file_name = WSLBatchGenerator.generate_file_name(dt_start, dt_end)
df_spark = WSLBatchGenerator.generate_wsl_df(spark, metadata, transformer, size=10**5)
wsl_ingestor.write_to_staging(df_spark, f"{path}/{file_name}")
