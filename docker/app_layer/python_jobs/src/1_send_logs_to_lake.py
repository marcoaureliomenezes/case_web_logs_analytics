import os
import logging

from datetime import datetime as dt
import boto3

from wsl_batch_generator import WSLBatchGenerator


class WSLIngestor:
  
    def __init__(self, logger):
        self.logger = logger

    def config_s3_client_conn(self, host, access_key, secret_key, bucket):
        self.s3 = boto3.client('s3',
        endpoint_url=host,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
        self.bucket = bucket
        return self
  
    def config_file_prefix(self, lake_path, execution_date):
        execution_date = dt.strptime(execution_date, '%Y-%m-%d %H:%M:%S%z')
        partitioned_path = dt.strftime(execution_date, 'year=%Y/month=%m/day=%d/hour=%H')
        paths = {"local": f"./tmp/{partitioned_path}", "s3": f"{lake_path}/{partitioned_path}"}
        _ = os.makedirs(paths["local"], exist_ok=True)
        self.logger.info(f"local_path:{paths['local']};lake_path:{paths['s3']}")
        return paths  


if __name__ == "__main__":
  
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.addHandler(logging.StreamHandler())

  S3_ENDPOINT = os.getenv("S3_ENDPOINT")
  ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
  SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
  EXECUTION_DATE = os.getenv("EXECUTION_DATE", "2024-10-21 03:00:00+00:00")
  BUCKET = os.getenv("BUCKET")
  PREFIX_PATH = os.getenv("PREFIX_PATH")
  


  logger.info(f"Execution Date: {EXECUTION_DATE}")
  logger.info(f"S3 Endpoint: {S3_ENDPOINT}")
  logger.info(f"Bucket: {BUCKET}")
  logger.info(f"Prefix Path: {PREFIX_PATH}")

  dt_execution_date = dt.strptime(EXECUTION_DATE, "%Y-%m-%d %H:%M:%S%z")
  hours_back = 6

  format, dt_start, dt_end = WSLBatchGenerator.handle_timestamp(dt_execution_date, hours_back)
  metadata = WSLBatchGenerator.metadata_case_web_log_server(format, dt_start, dt_end)
  transformer = WSLBatchGenerator.web_server_log_transformer

  wsl_ingestor = WSLIngestor(logger)
  paths = wsl_ingestor.config_file_prefix(PREFIX_PATH, EXECUTION_DATE)
  file_name = WSLBatchGenerator.generate_file_name(dt_start, dt_end)

  WSLBatchGenerator.generate_web_server_log_file(f"{paths['local']}/{file_name}", metadata, transformer, 10**6)

  wsl_ingestor.config_s3_client_conn(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET)
  wsl_ingestor.config_file_prefix(PREFIX_PATH, EXECUTION_DATE)
  wsl_ingestor.s3.upload_file(f"{paths['local']}/{file_name}", BUCKET, f"{paths['s3']}/{file_name}")