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
  f"dag_wsl_ingestion_per_6h",
  start_date=datetime(year=2024,month=10,day=30,hour=0),
  schedule_interval='0 */6 * * *',
  default_args=default_args,
  max_active_runs=2,
  catchup=True
  ) as dag:

    starting_process = BashOperator(
      task_id="starting_task",
      bash_command="""sleep 2"""
    )

    generate_ingest_log_server_1 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-python-apps:1.0.0",
      task_id="generate_ingest_log_server_1",
      entrypoint="python /app/wsl_batch_generator.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "raw-data",
      "PREFIX_PATH": "web-server-logs",
      "SERVER": "server-1",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )

    generate_ingest_log_server_2 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-python-apps:1.0.0",
      task_id="generate_ingest_log_server_2",
      entrypoint="python /app/wsl_batch_generator.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
     "BUCKET": "raw-data",
      "PREFIX_PATH": "web-server-logs",
      "SERVER": "server-2",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )
    
    generate_ingest_log_server_3 = DockerOperator(
      **COMMON_KWARGS_DOCKER_OPERATOR,
      image="wsl-python-apps:1.0.0",
      task_id="generate_ingest_log_server_3",
      entrypoint="python /app/wsl_batch_generator.py",
      environment= {
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
      "BUCKET": "raw-data",
      "PREFIX_PATH": "web-server-logs",
      "SERVER": "server-3",
      "EXECUTION_DATE": "{{ execution_date }}"                      
      }
    )


    end_process = BashOperator(
      task_id="end_process",
      bash_command="""sleep 2"""
    )


    starting_process >> [generate_ingest_log_server_1, generate_ingest_log_server_2, generate_ingest_log_server_3] >> end_process