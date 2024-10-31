import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


COMMON_KWARGS_DOCKER_OPERATOR = dict(
  network_mode="weblogs_lake_network",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  tty=False,
)

default_args ={
  "owner": "airflow",
  "email_on_failure": False,
  "email_on_retry": False,
  "email": "marco_aurelio_reis@yahoo.com.br",
  "retries": 1,
  "retry_delay": timedelta(minutes=5) 
}

with DAG(
  f"dag_hourly_wsl",
  start_date=datetime(year=2024,month=7,day=20,hour=2),
  schedule_interval="@hourly",
  default_args=default_args,
  max_active_runs=2,
  catchup=False
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )

    wsl_capture_and_ingest = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-python-apps:1.0.0",
      task_id="wsl_capture_and_ingest",
      entrypoint="python /app/1_send_logs_to_lake.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "staging",
      "PREFIX_PATH": "web-server-logs",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )
    
    wsl_ingest_to_bronze = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-spark-apps:1.0.0",
      task_id="wsl_ingest_to_bronze",
      entrypoint="sh /app/1_staging_to_bronze/spark-submit.sh",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "NESSIE_URI": os.getenv("NESSIE_URI"),
      "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "staging",
      "EXECUTION_DATE": "{{ execution_date }}",
      "STAGING_PATH": "s3a://staging/web-server-logs",
      "BRONZE_TABLE_NAME": "nessie.bronze.wsl_logs",
      "BRONZE_TABLE_PATH": "s3a://lakehouse/bronze/wsl_logs"
      }
    )


    wsl_bronze_to_silver= DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-spark-apps:1.0.0",
      task_id="wsl_bronze_to_silver",
      entrypoint="sh /app/2_bronze_to_silver/spark-submit.sh",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "NESSIE_URI": os.getenv("NESSIE_URI"),
      "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_REGION": os.getenv("AWS_DEFAULT_REGION"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "EXECUTION_DATE": "{{ execution_date }}",
      "BRONZE_TABLE_NAME": "nessie.bronze.wsl_logs",
      "SILVER_TABLE_NAME": "nessie.silver.wsl_logs",
      "SILVER_TABLE_PATH": "s3a://lakehouse/silver/wsl_logs"
      }
    )
    


    end_process = BashOperator(
      task_id="end_process",
      bash_command="""sleep 2"""
    )


    starting_process >> wsl_capture_and_ingest >> wsl_ingest_to_bronze >> wsl_bronze_to_silver >> end_process